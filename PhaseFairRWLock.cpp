#include <assert.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <condition_variable>

#include <functional>
#include <thread>
#include <vector>
#include <iostream>

class PhaseFairRWLock {
  struct WriterAlarmClock {
    std::condition_variable cond;
    WriterAlarmClock*       next;

    WriterAlarmClock() : next(nullptr) {
    }
  };

  std::mutex              _mutex;
  std::condition_variable _cond;
  int32_t                 _readersRun;
  int32_t                 _readersWait;
  WriterAlarmClock*       _sleepersStart;
  WriterAlarmClock*       _sleepersEnd;
  WriterAlarmClock*       _freeList;
  uint8_t                 _phase;

  typedef std::chrono::steady_clock clock_t;
  typedef std::chrono::duration<double> duration_t;
  typedef std::chrono::time_point<clock_t, duration_t> timepoint_t;
  static clock_t _clock;

  // Phases:
  //   0: readers, no writers waiting, this includes 0 readers
  //   1: readers, but writers waiting (_sleepersStart != nullptr)
  //   2: kicking off a writer (_sleepersStart != nullptr)
  //   3: writer active (_sleepersStart unknown)
  //
  // Usually, we go 0 -> 1 -> 2 -> 3 -> 0, with the following exceptions:
  //   - We can go 0 -> 3 if there are no readers left before a writer appears,
  //   - we can go 3 -> 2, if a writer unlocks, another one already has a 
  //     ticket and there is no reader waiting.
  //   - we can go 1 -> 0, if a tryWriteLock times out and there are no other
  //     writers.
  //
  // _sleepersStart and _sleepersEnd is a queue for sleeping writers,
  // the former points to the one whose turn it is next, the latter to
  // the last in the queue. If there are no sleeping writers, then
  // _sleepersStart and _sleepersEnd are nullptr. The _freeList keeps
  // previously allocated WriterAlarmClocks (only freed on destruction).
  //
  // Invariants: When nobody holds _mutex, the following invariants hold:
  //   - If _phase == 0, _sleepersStart == nullptr == _sleepersEnd
  //   - If _phase == 1 or 2, _sleepersStart != nullptr != _sleepersEnd
  //   - If _phase == 3, somebody has the write lock, and _sleepersStart
  //     might be nullptr (no other waiters) or not (other waiters waiting)
  //   - If _readersRun > 0, then _phase is 0 or 1
  //   - If _sleepersStart != nullptr, then it points to a finite chain of
  //     WriterAlarmClock structs, and _sleepersEnd points to the last of
  //     them.
  // These invariants must be maintained at all times and by all methods.

 public:
  PhaseFairRWLock() 
    : _readersRun(0), _readersWait(0),
      _sleepersStart(nullptr), _sleepersEnd(nullptr), _freeList(nullptr),
      _phase(0) {
  }

  ~PhaseFairRWLock() {
    std::unique_lock<std::mutex> guard(_mutex);
    // Note that we are intentionally leaking all WriterAlarmClocks which
    // are currently in use by somebody still waiting for the write lock.
    WriterAlarmClock* wac;
    WriterAlarmClock* wac2;
    
    for (wac = _freeList; wac != nullptr; wac = wac2) {
      wac2 = wac->next;
      delete wac;
    }
  }

  void writeLock() {
    std::unique_lock<std::mutex> guard(_mutex);

    // The fast path of a completely free RW lock:
    if (_phase == 0 && _readersRun == 0) {
      _phase = 3;
      return;
    }

    // Now we need an alarm clock:
    WriterAlarmClock* wac = getAlarmClock();

    // First check if the sleeper queue is non-empty:
    if (_sleepersStart != nullptr) {
      // Append our alarm clock to the back of the list:
      assert(_sleepersEnd != nullptr);
      _sleepersEnd->next = wac;
      _sleepersEnd = wac;
      // And wait:
      wac->cond.wait(guard,
          [&]() -> bool { return wac == _sleepersStart; });
      // When we wake up, we are at the beginning of the (non-empty) queue:
    } else {  // Put ourselves as unique alarm clock on the list:
      assert(_sleepersEnd == nullptr);
      _sleepersStart = wac;
      _sleepersEnd = wac;
    }

    assert(wac == _sleepersStart);

    if (_phase == 0) {
      _phase = 1;   // tell readers that we are waiting
    }

    // Wait for readers to finish (if necessary):
    wac->cond.wait(guard, [&]() -> bool { return _phase == 2; });

    // Got the write lock:
    _phase = 3;
    
    assert(wac == _sleepersStart);

    // Remove ourselves from the start of the queue:
    removeFromQueue(wac);
    returnAlarmClock(wac);
  }

  bool tryWriteLock() {
    std::unique_lock<std::mutex> guard(_mutex);

    // First check that there is no writer waiting:
    if (_sleepersStart != nullptr) {
      return false;
    }

    // Note that by the invariants, we now know that _phase == 0 or _phase == 3
    if (_phase == 0 && _readersRun == 0) {
      // Got the write lock:
      _phase = 3;
      return true;
    }
    return false;
  }

  // Returns true if the write lock was acquired and false if it timed out.
  // Timeout is in seconds.
  bool tryWriteLock(double timeout) {
    std::unique_lock<std::mutex> guard(_mutex);

    // The fast path of a completely free RW lock:
    if (_phase == 0 && _readersRun == 0) {
      _phase = 3;
      return true;
    }

    // Now we need an alarm clock:
    WriterAlarmClock* wac = getAlarmClock();

    // ... and a deadline
    timepoint_t now = _clock.now();
    timepoint_t deadline = now + duration_t(timeout);

    // First check if the sleeper queue is non-empty:
    if (_sleepersStart != nullptr) {
      // Append our alarm clock to the back of the list:
      _sleepersEnd->next = wac;
      _sleepersEnd = wac;

      // And wait:
      if (!wac->cond.wait_until(guard, deadline,
                                [&]() -> bool {
                                  return wac == _sleepersStart;
                                })) {
        removeFromQueue(wac);
        return false;
      }
      // When we wake up, we are at the beginning of the (non-empty) queue or
      // we have timed out.
    } else {  // Put ourselves as unique alarm clock on the list:
      assert(_sleepersEnd == nullptr);
      _sleepersStart = wac;
      _sleepersEnd = wac;
    }

    assert(wac == _sleepersStart);

    if (_clock.now() > deadline) {
      removeFromQueue(wac);
      return false;
    }

    if (_phase == 0) {
      _phase = 1;   // tell readers that we are waiting
    }

    // Wait for readers to finish (if necessary):
    if (!wac->cond.wait_until(guard, deadline,
                              [&]() -> bool { return _phase == 2; })) {
      // Timed out, need to repair things:
      removeFromQueue(wac);
      if (_phase == 1 && _sleepersStart == nullptr) {
        _phase = 0;
        if (_readersWait > 0) {
          _cond.notify_all();
        }
      }
      return false;
    }

    // Got the write lock:
    _phase = 3;

    // Remove ourselves from the start of the queue:
    removeFromQueue(wac);
    returnAlarmClock(wac);

    return true;
  }

  void unlockWrite() {
    std::unique_lock<std::mutex> guard(_mutex);
    unlockWriteWithMutex();
  }

  void readLock() {
    std::unique_lock<std::mutex> guard(_mutex);
    // Fast path:
    if (_phase == 0) {
      _readersRun++;
      return;
    }
    // Wait until next reading phase begins:
    _readersWait++;
    _cond.wait(guard, [&]() -> bool { return _phase == 0; });
    _readersWait--;
    _readersRun++;
    return;
  }

  bool tryReadLock() {
    std::unique_lock<std::mutex> guard(_mutex);
    if (_phase == 0) {
      _readersRun++;
      return true;
    }
    return false;
  } 

  bool tryReadLock(double timeout) {
    std::unique_lock<std::mutex> guard(_mutex);
    // Fast path:
    if (_phase == 0) {
      _readersRun++;
      return true;
    }

    // We need a deadline
    timepoint_t now = _clock.now();
    timepoint_t deadline = now + duration_t(timeout);

    // Wait until next reading phase begins:
    _readersWait++;
    if (!_cond.wait_until(guard, deadline,
                          [&]() -> bool { return _phase == 0; })) {
      _readersWait--;
      return false;
    }
    _readersWait--;
    _readersRun++;
    return true;
  }

  void unlockRead() {
    std::unique_lock<std::mutex> guard(_mutex);
    unlockReadWithMutex();
  }

  void unlock() {
    std::unique_lock<std::mutex> guard(_mutex);
    // We assume that we either have the read lock or the write lock,
    // we can read this off from the phase:
    if (_phase == 3) {
      unlockWriteWithMutex();
    } else {
      unlockReadWithMutex();
    }
  }

 private:
  // All of these must be called under the _mutex:
  WriterAlarmClock* getAlarmClock() {
    WriterAlarmClock* wac = _freeList;
    if (wac != nullptr) {
      _freeList = _freeList->next;
      wac->next = nullptr;
      return wac;
    }
    return new WriterAlarmClock();
  }

  void returnAlarmClock(WriterAlarmClock* wac) {
    assert(wac != nullptr);
    wac->next = _freeList;
    _freeList = wac;
  }

  void removeFromQueue(WriterAlarmClock* wac) {
    // At the start?
    if (wac == _sleepersStart) {
      _sleepersStart = _sleepersStart->next;
      if (_sleepersStart == nullptr) {
        _sleepersEnd = nullptr;
      }
      return;
    }
    // Now wac can be in the queue or not, but not in first position:
    WriterAlarmClock* w = _sleepersStart;
    while (w != nullptr) {
      if (w->next == wac) {
        w->next = wac->next;
        if (w->next == nullptr) {
          _sleepersEnd = w;
        }
        return;
      }
      w = w->next;
    }
  }

  void unlockWriteWithMutex() {
    // Invariant: When this is called, _phase == 3.
    assert(_phase == 3);

    if (_readersWait > 0) {
      _phase = 0;
      _cond.notify_all();
    } else if (_sleepersStart != nullptr) {
      _phase = 2;
      _sleepersStart->cond.notify_one();
    } else {
      _phase = 0;
    }
  }

  void unlockReadWithMutex() {
    // Invariant: When this is called, _phase can only be 0 (no writers waiting)
    // or 1 (writers waiting).
    if (--_readersRun == 0) {
      if (_phase == 1) {
        _phase = 2;
        _sleepersStart->cond.notify_one();
        return;
      }
    }
  }

};

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "usage: " << argv[0] << " iterations concurrency" << std::endl;
    std::abort();
  }

  int const count = std::stoll(argv[1]);
  int const concurrency = std::stoll(argv[2]);
  
  std::cout << "ITERATIONS: " << count << ", WRITE CONCURRENCY: " << concurrency << std::endl;
  
  PhaseFairRWLock lock;
  int value = 0;
  
  std::cout << "VALUE AT START IS: " << value << std::endl;

  std::vector<std::thread> threads;
  threads.reserve(20);

  auto func = [&lock, &value](int id, int iterations, bool report) {
    int const n = iterations / 20;
    for (int i = 0; i < iterations; ++i) {
      if (report && (i % n == 0)) {
        std::cout << "#" << id << " ITERATIONS: " << i << "\n";
      }

      lock.writeLock();
      ++value;
      lock.unlockWrite();
    }
  };
 
  for (int i = 0; i < concurrency; ++i) { 
    threads.emplace_back(func, i, count, i >= 0);
  }
  
  for (auto& it : threads) {
    it.join();
  }

  std::cout << "VALUE AT END IS: " << value << std::endl;
}
