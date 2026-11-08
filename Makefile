# Makefile for OS Final Project — Parallel CSV Pipeline (C++ Version)
# Builds four executables: dispatcher, ingester, processor, reporter

CXX      = g++
CXXFLAGS = -Wall -Wextra -pthread -Isrc -std=c++11
LDFLAGS  = -pthread -lrt

SRC_DIR = src

TARGETS = dispatcher ingester processor reporter

.PHONY: all clean

all: $(TARGETS)

dispatcher: $(SRC_DIR)/dispatcher.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

ingester: $(SRC_DIR)/ingester.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

processor: $(SRC_DIR)/processor.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

reporter: $(SRC_DIR)/reporter.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS)
	rm -f /tmp/os_proj_fifo
	rm -f /dev/shm/os_proj_shm /dev/shm/sem.os_proj_ready_sem 2>/dev/null || true
