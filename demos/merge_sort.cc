#include <cstdint>
#include <cassert>
#include <memory>
#include <string>
#include <tuple>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <random>
#include <algorithm>

// Seastar framework support.
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/file.hh>

using namespace std::chrono_literals;

// Demonstrate multiple sorting implementatios
// Notes:
// 1. most errors are ignored. Assertions provide basic sanity protection.

#define LOG(x) do {x; std::cout << std::endl;} while (0);

// Binary data provides extreme randomess buut is hard to debug & visualize.
// Textual data is easily visualized for verification.
#define GEN_TEXT true

// The size of a single record.
// Notice: This is an aligned size, so no need to worry about alignment when doing DMA based IO.
static constexpr uint64_t kRecordSize = 4 * 1024;

// How many numbers are in a single record.
static constexpr uint64_t kItemsInRecord = kRecordSize / sizeof(uint64_t);

struct Record {
	uint64_t 	data[kItemsInRecord];


	// Fill random binary data.
	void FillBinaryRandom(std::mt19937&	gen,
		       		std::uniform_int_distribution<>& dist) {
		for (uint64_t& elem : data) {
			elem = dist(gen);
		}
	}

	// Make record a set of text lines.
	void FillTextualRandom(std::mt19937&	gen,
		       		std::uniform_int_distribution<>& dist) {
		memset(data, ' ', sizeof(data));
		constexpr int kLineSize = 50;
		char *text = reinterpret_cast<char*>(data);
		for (int offset = kLineSize; offset < sizeof(data); offset += kLineSize) {
			text[offset] = '\n';
		}
		// Write a textual string in record header, to make it easy to look into their order.
		int written = sprintf(text, "Record Number: %04d\n", dist(gen) % 1000);
		text[written] = ' ';
	}

	void FillRandom(std::mt19937&	gen,
		 	std::uniform_int_distribution<>& dist) {
#if GEN_TEXT
		FillTextualRandom(gen, dist);
#else
		FillBinaryRandom(gen, dist);
#endif
	}

	void Read(std::ifstream& file) {
		file.read(reinterpret_cast<char*>(data), sizeof(data));
	}

	void Write(std::ofstream& file) {
		file.write(reinterpret_cast<const char*>(data), sizeof(data));
	}

	// Compare 2 recods as in std::less<>
	friend bool operator<(const Record& r1, const Record& r2);
};
static_assert(sizeof(Record) == kRecordSize);

bool operator<(const Record& r1, const Record& r2) {
#if GEN_TEXT
	const char* p1 = reinterpret_cast<const char*>(r1.data);
	const char* p2 = reinterpret_cast<const char*>(r2.data);
	for (int i=0 ; i < sizeof(r1.data); ++i, ++p1, ++p2) {
		if (*p1 < *p2) {
			return true;
		}
		if (*p1 > *p2) {
			return false;
		}
	}
#else
	const uint64_t* p1 = r1.data;
	const uint64_t* p2 = r2.data;
	for (int i=0 ; i < kItemsInRecord; ++i, ++p1, ++p2) {
		if (*p1 < *p2) {
			return true;
		}
		if (*p1 > *p2) {
			return false;
		}
	}
#endif
	// Both are equal - anything is OK.
	return false;
}

// Use a large random buffer to write large chunks.
static constexpr int kRandomBufferSize = 1024 * 1024;

// Verify the random buffer contains whole records only.
static_assert(kRandomBufferSize % kRecordSize == 0);

static constexpr uint64_t kRecordsInBuffer = kRandomBufferSize / kRecordSize;

// Only a a larger number of records than threshold will use intermediate files for MergeSort.
// Any number up this this thrshold, will be sorted in-memory, in one chunk.
static constexpr uint64_t kRecordsSortedInMemoryThreshold = 16;

// Given a record count to sort, decide how many records will be part of the left (respectively right) sub-computation.
uint64_t LeftHalfSize(uint64_t	record_count) {
	return record_count / 2;
}
uint64_t RightHalfSize(uint64_t	record_count) {
	return record_count - record_count / 2;
}

// Get the number of records in a file, verifies whole records exist.
uint64_t GetRecordsInFile (const std::string&  path) {
	std::filesystem::path p(path);
	uint64_t file_size = std::filesystem::file_size(p);
	assert(file_size % kRecordSize == 0);
	return file_size / kRecordSize;
}

// Reads a range of records, from file to memory.
// Range is [first_record_index, last_record_index)
// Beware of parallel access - it uses seek to reposition the file.
void ReadFileRange(std::ifstream&	file,
		   Record*		buf,
		   uint64_t 		first_record_index,
		   uint64_t		last_record_index) {
	const uint64_t record_count = last_record_index - first_record_index;
	file.seekg(kRecordSize * first_record_index);
	file.read(reinterpret_cast<char*>(buf), kRecordSize * record_count);	
}

// Check if records within a file are sorted properly
bool IsFileRecordsSorted(const std::string& file_name) {
	const uint64_t record_count = GetRecordsInFile(file_name);	

	std::vector<Record> buf(record_count);

	// Read input into memory.
	std::ifstream in_file;
  	in_file.open(file_name);
	ReadFileRange(in_file, buf.data(), 0, record_count);
  	in_file.close();

	// Check.
	return std::is_sorted(buf.begin(), buf.end());
}

// Creates a file containing random records.
// To be used as sample input for the sorting code.
// NOTE: we create a large random buffer & write it many times.
void CreateRandomFile(const std::string 	filename,
		  uint64_t			num_records) {
	std::random_device	rd;  		// Seed source.
	std::mt19937		gen{rd()}; 	// RNG
	std::uniform_int_distribution<>	dist{0, 1000000}; // Distribution generator.
	std::vector<Record>		buf(kRecordsInBuffer);

	int record_count = 0;
	for (Record &r : buf) {
		r.FillRandom(gen, dist);
	}

	std::ofstream myfile;
	myfile.open(filename);
	while (num_records > 0) {
		int current_write_count = std::min(kRecordsInBuffer, num_records);
		myfile.write(reinterpret_cast<const char*>(buf.data()),
					 kRecordSize * current_write_count);
		num_records -= current_write_count;
	}
 	myfile.close();
}


	// Assumes size fits in memory.
	// Used as the recursion termination case, for small ranges.
	// Sorting in memory is also used for verfication, since we use the std C++ library API.
	void InMemorySort (const std::string& input,
			   const std::string& output,
			   uint64_t           first_record_index,
			   uint64_t           last_record_index) {

		uint64_t record_count = last_record_index - first_record_index;
		std::vector<Record> buf(record_count);

		// Read input into memory.
		std::ifstream in_file;
  		in_file.open(input);
		ReadFileRange(in_file, buf.data(), 0, record_count);
   		in_file.close();

		// Sort.
		std::sort(buf.begin(), buf.end());

		// Write sorted output.
		std::ofstream out_file;
  		out_file.open(output);
		out_file.write(reinterpret_cast<const char*>(buf.data()),
					 kRecordSize * record_count);
   		out_file.close();
	}

	void InMemorySort (const std::string& input,
			   const std::string& output) {
		uint64_t record_count = GetRecordsInFile(input);
		InMemorySort(input, output, 0, record_count);
	}

	// Assumes size fits in memory.
	seastar::future<> SeastarInMemorySort (const std::string_view input,
			   const std::string_view output,
			   uint64_t           first_record_index,
			   uint64_t           last_record_index) {

		uint64_t record_count = last_record_index - first_record_index;
		uint64_t aligned_size = kRecordSize * record_count;

		std::cout << "Start in-memory async IO sort:" << input << "[" << first_record_index << ","
		       << last_record_index << ") -> " << output << std::endl;
		// Read input into memory.
	    	return seastar::with_file(seastar::open_file_dma(input, seastar::open_flags::ro),
				[output, record_count, first_record_index, aligned_size] (seastar::file& f) {
			std::unique_ptr<std::vector<Record>> buf = std::make_unique<std::vector<Record>>(record_count);
			return f.dma_read(/*byte_offset=*/first_record_index * kRecordSize, buf->data(), aligned_size)
				.then([output, buf = std::move(buf), aligned_size] (size_t count) mutable {
				assert(count == aligned_size);

				// Sort.
				std::sort(buf->begin(), buf->end());

				// Open output file.
				return seastar::with_file(seastar::open_file_dma(
					output, seastar::open_flags::create | seastar::open_flags::wo),
					[buf = std::move(buf), aligned_size] (seastar::file& f) mutable {
					// Write the sorted output.
					return f.dma_write(/*byte_offset=*/0, buf->data(), aligned_size)
						.then([buf = std::move(buf), f, aligned_size] (size_t count) mutable {
						std::cout << "Completed in-memory async IO merge:" << aligned_size << std::endl;
						// We std::move() the buf into this lambda so it is kept alive during the write.
						assert(count == aligned_size);
						// Flush the file.
						return f.flush().then([f] () mutable {
							// Enclosed in thin `with_file()`, we don't need to explicitly close the 2 files.
                        			});
					});
				});
			});
		});
	}

	// Merge phase of file-based MergeSort.
	// No attempt to prefetch data or to read-ahead.
	void MergeFiles(const std::string&	left_name,
			const std::string&	right_name,
			const std::string&	output_name,
			uint64_t 		record_count) {
		std::cout << "Merging 2 file: " << left_name << ", " << right_name << std::endl;
		struct Reader {
			Reader(const std::string& file_name, uint64_t num_records) 
			  : num_records(num_records), file(file_name) {
				record.Read(file);
			}

			void Next() {
				assert(index < num_records);
				index++;
				if (IsDone())
					return;
				record.Read(file);
			}

			bool IsDone() const {
				return index >= num_records;
			}

			const uint64_t	num_records;
			uint64_t 	index = 0; // Points at the record being compared, or beyond last one.
			Record		record;
			std::ifstream 	file;
		};

		Reader l(left_name, LeftHalfSize(record_count));
		Reader r(right_name, RightHalfSize(record_count));
		std::ofstream output(output_name);
		while (!l.IsDone() || !r.IsDone()) {
			// Is one file exhausted ?
			if (l.IsDone()) {
				r.record.Write(output);
				r.Next();
				continue;
			}
			if (r.IsDone()) {
				l.record.Write(output);
				l.Next();
				continue;
			}
			// Neither one is exhausted - compare to pick next.
			if (l.record < r.record) {
				l.record.Write(output);
				l.Next();
			} else {
				r.record.Write(output);
				r.Next();
			}
		}
		assert(l.IsDone() && r.IsDone());
		assert(l.index + r.index == record_count);
	}


	// Seastar DMA based, Async IO reader of a file that is already sorted.
	struct DMAReader {
		DMAReader(seastar::file f, uint64_t num_records) 
		  : num_records(num_records), file(f) {}

		// Issues a read from a file.
		seastar::future<size_t> DMARead(uint64_t index) {
			return file.dma_read(/*byte_offset=*/index * kRecordSize,
				       		reinterpret_cast<char*>(record.data),
					       	kRecordSize);
		}

		// Complete the ctor by creating the future to comlete the reading of the first block.
		seastar::future<size_t> Init() {
			assert(index == 0);
			return DMARead(0);
		}

		seastar::future<size_t> NextFuture() {
			std::cout << "NextFuture called: this="<< this << ", index=" << index << ", num_records=" << num_records << std::endl;
			assert(index < num_records);
			index++;
			if (IsDone()) {
				std::cout << "NextFuture NOT issuing another read !!!" << std::endl;
				// Verify we only get here once, notice weve consumed the file & wont try to use it again.
				assert(index == num_records);
				return seastar::make_ready_future()
					// Notice: return the record size only because our simple error check asserts
					// that write of a record reports writing its size.
					.then([]{return size_t{kRecordSize};});
			}
			return DMARead(index);
		}

		// The index points at the record we have in memory.
		// When it points past the last record index, were done processing this file.
		bool IsDone() const {
			return index >= num_records;
		}

		const uint64_t	num_records;
		uint64_t 	index = 0; // Points at the record being compared, or beyond last one.
		Record		record;
		seastar::file  	file;
	};

	// Merge 2 sorted files into an output file, using seastart async IO.
	class AsyncMerger {
	public:
#if 0
		// nullify the object.
		AsyncMerger() : is_nullified_(true) {}
#endif

		// Copy all file objects.
		AsyncMerger(const seastar::file& left_file,
				const seastar::file& right_file,
				const seastar::file& output_file,
				const std::string output_name,
				uint64_t             record_count)
		: is_nullified_(false), left_reader_(left_file, LeftHalfSize(record_count)),
	       		right_reader_(right_file, RightHalfSize(record_count)),
	       		left_file_(left_file),
		       	right_file_(right_file),
		       	output_file_(output_file),
			output_name_(output_name),
		       	record_count_(record_count) {}

		// Initiate async read from both files.
		auto Init() {
			auto left_read = left_reader_.Init();
			auto right_read = right_reader_.Init();
			//assert(is_left_read);
			//assert(is_right_read);
			// Notice: remember to use the results of the 2 IO's.
			return when_all_succeed(std::move(left_read), std::move(right_read));
		}

		// Assumes that there are still records to merge.
		// Process a single record:
		// 1. Pick the next record
		// 2. Create a future that is ready when the picked record was written to the output file).
		// 	It will then issue a read to get the next record.
		// So the chain is read when we wrote one record & read the next from the file that was consumed.
		auto processOneRecord () {
			assert(!IsDone());
			char *data_to_write = nullptr;
			DMAReader *next_reader = nullptr;
			seastar::future read_next = seastar::make_ready_future();
			// Is one file exhausted ?
			if (left_reader_.IsDone()) {
				data_to_write = reinterpret_cast<char*>(right_reader_.record.data);
				next_reader = &right_reader_;
			} else if (right_reader_.IsDone()) {
				data_to_write = reinterpret_cast<char*>(left_reader_.record.data);
				next_reader = &left_reader_;
			} else 
			// Neither one is exhausted - compare to pick next.
			if (left_reader_.record < right_reader_.record) {
				data_to_write = reinterpret_cast<char*>(left_reader_.record.data);
				next_reader = &left_reader_;
			} else {
				data_to_write = reinterpret_cast<char*>(right_reader_.record.data);
				next_reader = &right_reader_;
			}
			assert(data_to_write != nullptr);
			assert(next_reader != nullptr);
			// Write the record, so we can reuse the buffer.
			return output_file_.dma_write(/*byte_offset=*/write_offset_, data_to_write, kRecordSize)
				.then([this, next_reader] (size_t count) {
				std::cout << "Comlpeted writing next merge record: this=" << this
			       		<< ", output_name=" << output_name_ << ", offset="
					<< (write_offset_ / kRecordSize) << ", count=" << count
				       	<< ", next_reader->index=" << next_reader->index
				       	<< ", reader->num_records=" << next_reader->num_records << std::endl;
				assert(count == kRecordSize);
				this->write_offset_ += kRecordSize;
				// Read the next record - Only reads when reader hasnt exhausted its file.
				auto read_record = next_reader->NextFuture();
				return std::move(read_record).then([this] (size_t count) {
					std::cout << "Comlpeted reading next record: this=" << this << ", count=" << count << std::endl;
					assert(count == kRecordSize);
					return seastar::make_ready_future();
				});
			});
		}

		bool IsDone() const {
			return (left_reader_.IsDone() && right_reader_.IsDone());
		}

		seastar::future<> close () {
			if (is_nullified_)
				return seastar::make_ready_future();
			// Verify both source files were fully consumed.
			assert(IsDone());
			assert(left_reader_.index + right_reader_.index == record_count_);
			std::cout << "Closing merge file:" << std::endl;
			// First flush all files.
			return when_all_succeed(left_file_.flush(), right_file_.flush(), output_file_.flush())
				.then_unpack([lf = left_file_, rf = right_file_, of = output_file_] () mutable {
					// Once flushed, close.
					return when_all(lf.close(), rf.close(), of.close()).discard_result();
			});

			std::cout << "Closed merge file:" << std::endl;
		}

		~AsyncMerger() {}

	private:
		const bool 	is_nullified_;
		DMAReader 	left_reader_;
		DMAReader 	right_reader_;
		seastar::file 	left_file_;
		seastar::file 	right_file_;
		seastar::file 	output_file_;
		uint64_t      	record_count_;
		uint64_t	write_offset_ = 0;	
		// Debugging aids.
		const std::string output_name_;
	};

	// Merge phase of file-based MergeSort using seastar async file IO.
	// No attempt to prefetch data or to read-ahead or buffer writes, .....
	seastar::future<> FutureBasedMergeFiles(std::unique_ptr<AsyncMerger>&& merger,
			const /*seastar::sstring*/std::string	left_name,
			const /*seastar::sstring*/std::string	right_name,
			const /*seastar::sstring*/std::string	output_name,
			uint64_t 		record_count) {
// Allow choosing merge phase between parallel & serial code, for comparison.
#define USE_SEASTAR true
#if !USE_SEASTAR
		return seastar::make_ready_future()
			.then([merger = std::move(merger),
				left_name = std::move(left_name),
				right_name = std::move(right_name),
			       	output_name = std::move(output_name),
			       	record_count] {
			MergeFiles(left_name, right_name, output_name, record_count);
			// Close files after merge has completed.
			return merger->close();
		});
#else

		return merger->processOneRecord().then([merger = std::move(merger), record_count] () mutable {
			// Are we done?
			if (merger->IsDone()) {
				// Close files after merge has completed.
				return merger->close();
			}
			// Continue merging.
			return FutureBasedMergeFiles(std::move(merger), "", "", "", record_count);
		});
#endif
	}

	// Open files in prep for merge phase of sorting.
	seastar::future<> FutureBasedMergeOpenFiles(const std::string	left_name,
						const std::string	right_name,
						const std::string	output_name,
						uint64_t 		record_count) {
		uint64_t aligned_size = kRecordSize * record_count;

		// Open the 2 input files & single output file.
		std::cout << "Try open 3 files: " << left_name << ", " << right_name << ", " <<  output_name << std::endl;
		auto left_f = seastar::open_file_dma(left_name, seastar::open_flags::ro);
		auto right_f = seastar::open_file_dma(right_name, seastar::open_flags::ro);
	       	auto output_f = seastar::open_file_dma(output_name, seastar::open_flags::create | seastar::open_flags::wo);	
		return seastar::when_all_succeed(std::move(left_f), std::move(right_f), std::move(output_f))
			.then_unpack([left_name, right_name, output_name, record_count]
				(const seastar::file& left_file,
				 const seastar::file& right_file,
				 const seastar::file& output_file) mutable {
			auto merger = std::make_unique<AsyncMerger>(left_file, right_file, output_file, output_name, record_count);
			// Fire the init which does the initial read of first block from each file.
			return merger->Init().then_unpack([merger = std::move(merger), left_name = std::move(left_name),
				       right_name = std::move(right_name), output_name = std::move(output_name),
				       record_count] (size_t left_size, size_t right_size) mutable {
				std::cout << "initial read of both source files completed - start merge: "
			       		<< left_name << ", " << right_name << std::endl;
				// Verify the initial read size.
				assert(left_size == kRecordSize);
				assert(right_size == kRecordSize);
				return FutureBasedMergeFiles(std::move(merger), std::move(left_name), std::move(right_name),
					       			std::move(output_name), record_count);
			});
		});
	}

	// Merge sort with a range of [start, end)
	void SerialMergeFileSort(const std::string& input_name,
				const std::string&  output_name,
				uint64_t         	first_record_index,
       				uint64_t         	last_record_index) {
		LOG(std::cout << "SerialMergeFileSort: [" << first_record_index << " , " << last_record_index << ")");
		assert(first_record_index <= last_record_index);
		// Decide between in-memory vs. file based sorting.
		uint64_t record_count = last_record_index - first_record_index;
		if (record_count <= kRecordsSortedInMemoryThreshold) {
			// Small vector is sorted in memory.
			InMemorySort(input_name, output_name, first_record_index, last_record_index);
			return;
		}

		// Recurse, half file size per call.
		const std::string left_name = output_name + "-l";
		const std::string right_name = output_name + "-r";
		const uint64_t pivot_index = first_record_index + LeftHalfSize(record_count); // First index of right half.
		SerialMergeFileSort(input_name, left_name, first_record_index, pivot_index);
		SerialMergeFileSort(input_name, right_name, pivot_index, last_record_index);

		// Merge the 2 sorted halves from separate files by streaming.
		MergeFiles(left_name, right_name, output_name, record_count);
	}

	// Mergesort which is based on merging files.
	void SerialMergeFileSort(const std::string& 	input,
				 const std::string& 	output) {
		uint64_t record_count = GetRecordsInFile(input);
		SerialMergeFileSort(input, output, 0, record_count);
	}

	seastar::future<>
       	FutureBasedMergeFileSort (const std::string&	input_name,
			       const std::string&  	output_name,
			       uint64_t            	first_record_index,
                  	       uint64_t         	last_record_index) {
		LOG(std::cout << "FutureBasedMergeFileSort starts: [" << first_record_index << " , " << last_record_index << ")");
		assert(first_record_index <= last_record_index);
		// Decide between in-memory vs. file based sorting.
		uint64_t record_count = last_record_index - first_record_index;
		if (record_count <= kRecordsSortedInMemoryThreshold) {
			// Small vector is sorted in memory.
			return SeastarInMemorySort(input_name, output_name, first_record_index, last_record_index)
				.then([first_record_index, last_record_index]{
				LOG(std::cout << "FutureBasedMergeFileSort ends: [" << first_record_index << " , " << last_record_index << ")");
			});
		}

		// Recurse, half file size per call.
		const std::string left_name = output_name + "-l";
		const std::string right_name = output_name + "-r";
		const uint64_t pivot_index = first_record_index + LeftHalfSize(record_count); // First index of right half.

		// Create 2 futures to sort both halves in parallel on separate physical cores.
		// In reality, splitting to the number of available cores will allow this to spread evenly across all available cores.
		// That's a K-Merge Sort !!!
		auto left_sort = seastar::smp::submit_to(/*cpu_nr=*/0,
			       	[input_name, left_name, first_record_index, pivot_index] {
			// Recursive call.
			return FutureBasedMergeFileSort(input_name, left_name, first_record_index, pivot_index);
		});
		auto right_sort = seastar::smp::submit_to(/*cpu_nr=*/1,
			       	[input_name, right_name, last_record_index, pivot_index] {
			// Recursive call.
			return FutureBasedMergeFileSort(input_name, right_name, pivot_index, last_record_index);
		});
		// Wait for both halves to comlpete before merging them.
		return seastar::when_all_succeed(std::move(left_sort), std::move(right_sort))
				.then_unpack([left_name, right_name, output_name, record_count,
				       first_record_index, last_record_index](/*auto results*/){
			// Merge the 2 sorted halves from separate files by streaming.
			return FutureBasedMergeOpenFiles(left_name, right_name, output_name, record_count).then([first_record_index, last_record_index]{
				LOG(std::cout << "FutureBasedMergeFileSort ends: [" << first_record_index << " , " << last_record_index << ")");
			});
		}).discard_result();
	}

	// Seastar Futures based Mergesort which is based on merging files.
	seastar::future<> FutureBasedMergeFileSort(const std::string& 	input,
						 const std::string& 	output) {
		uint64_t record_count = GetRecordsInFile(input);
		return FutureBasedMergeFileSort(input, output, 0, record_count);
	}


void test_serial_merge_sort () {
	std::vector</*record_count=*/uint64_t> test_cases = {
		10, 100 , 1000	
	};
	const std::string kTestFileNamePrefix = "/tmp/input-"; 

	for (auto record_count : test_cases) {
		const std::string input_file_name = kTestFileNamePrefix + std::to_string(record_count) + ".data";
		CreateRandomFile(input_file_name, record_count);
		assert(GetRecordsInFile(input_file_name) == record_count);
		assert(!IsFileRecordsSorted(input_file_name)); // Very unlikely to fail.

		// In memory sorting.
		std::cout << "Test in-memory sort: record_count=" << record_count << std::endl;
		const std::string in_mem_output_file = "/tmp/output-memory-" + std::to_string(record_count) + ".data";
		InMemorySort(input_file_name, in_mem_output_file);
		assert(IsFileRecordsSorted(in_mem_output_file));

		// Serial merge sort
		std::cout << "Test serial merge-sort: record_count=" << record_count << std::endl;
		const std::string serial_merge_sort_output_file = "/tmp/output-serial-merge-" + std::to_string(record_count) + ".data";
		SerialMergeFileSort(input_file_name, serial_merge_sort_output_file);
		assert(IsFileRecordsSorted(serial_merge_sort_output_file));
	}

	// Test all sizes of vectors, from 1 to in-memory threshold, for both left & right halves.
	for (int record_count = 1; record_count <= 2 * kRecordsSortedInMemoryThreshold + 1 ; ++record_count) {
		std::cout << "Test serial merge-sort mini sizes: record_count=" << record_count << std::endl;
		const std::string input_file_name = "/tmp/small-sizes-in-" + std::to_string(record_count) + ".data";
		CreateRandomFile(input_file_name, record_count);
		assert(GetRecordsInFile(input_file_name) == record_count);

	 	const std::string output_file_name = "/tmp/small-sizes-out-" + std::to_string(record_count) + ".data";
		SerialMergeFileSort(input_file_name, output_file_name);
		assert(IsFileRecordsSorted(output_file_name));
	}
}




seastar::future<> test_future_based_seastar_merge_sort () {
	// Return a future for immediate execution?
	return seastar::make_ready_future().then([] {
		std::cout << "Seastar SMP cores:" << seastar::smp::count << "\n";
		uint64_t record_count = 1000;

		std::cout << "Test future-based merge-sort: record_count=" << record_count << std::endl;
		const std::string input_file_name = "/tmp/future-based-in-" + std::to_string(record_count) + ".data";
		CreateRandomFile(input_file_name, record_count);
		assert(GetRecordsInFile(input_file_name) == record_count);
		// Verify that the initial file is indeed un-ordered - this can rearely fail with epsilon probability.
		assert(!IsFileRecordsSorted(input_file_name));

		const std::string output_file_name = "/tmp/future-based-out-" + std::to_string(record_count) + ".data";
		return FutureBasedMergeFileSort(input_file_name, output_file_name).then([output_file_name] {
			assert(IsFileRecordsSorted(output_file_name));
		});
	});
}


int main(int argc, char** argv) {

	const bool run_serial_code = (argc == 1 && std::string("--serial") == argv[0]);
	if (run_serial_code) {
		std::cout << "Run tests in main" << std::endl;
		test_serial_merge_sort();
	}

	seastar::app_template app;
	app.run(argc, argv, [run_serial_code] {
		if (run_serial_code) {
			std::cout << "Run serial tests from within seastar" << std::endl;
			test_serial_merge_sort();
		}
		return test_future_based_seastar_merge_sort();
	});
	return 0;
}

