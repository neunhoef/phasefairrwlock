all:	PhaseFairRWLock

PhaseFairRWLock: PhaseFairRWLock.cpp
	g++ -g -std=c++11 -O2 -Wall -Wextra PhaseFairRWLock.cpp -pthread -o PhaseFairRWLock
