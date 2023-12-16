#!/bin/bash
#
# Build the merge_sort.cc demo program
#
# Notice: Run the program with `sudo` !!!
#
# TODO: Consider making this an officla demo, with Makefile support
clang++ ./merge_sort.cc  `pkg-config --cflags --libs --static ../build/release/seastar.pc` -o /tmp/merge_sort
