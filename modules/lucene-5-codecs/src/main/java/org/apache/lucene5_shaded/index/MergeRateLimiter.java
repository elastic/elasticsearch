/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.index;


import org.apache.lucene5_shaded.store.RateLimiter;
import org.apache.lucene5_shaded.util.ThreadInterruptedException;

/** This is the {@link RateLimiter} that {@link IndexWriter} assigns to each running merge, to 
 *  give {@link MergeScheduler}s ionice like control.
 *
 *  This is similar to {@link SimpleRateLimiter}, except it's merge-private,
 *  it will wake up if its rate changes while it's paused, it tracks how
 *  much time it spent stopped and paused, and it supports aborting.
 *
 *  @lucene.internal */

public class MergeRateLimiter extends RateLimiter {

  private final static int MIN_PAUSE_CHECK_MSEC = 25;
  volatile long totalBytesWritten;

  double mbPerSec;
  private long lastNS;
  private long minPauseCheckBytes;
  private boolean abort;
  long totalPausedNS;
  long totalStoppedNS;
  final MergePolicy.OneMerge merge;

  /** Returned by {@link #maybePause}. */
  private static enum PauseResult {NO, STOPPED, PAUSED};

  /** Sole constructor. */
  public MergeRateLimiter(MergePolicy.OneMerge merge) {
    this.merge = merge;

    // Initially no IO limit; use setter here so minPauseCheckBytes is set:
    setMBPerSec(Double.POSITIVE_INFINITY);
  }

  @Override
  public synchronized void setMBPerSec(double mbPerSec) {
    // 0.0 is allowed: it means the merge is paused
    if (mbPerSec < 0.0) {
      throw new IllegalArgumentException("mbPerSec must be positive; got: " + mbPerSec);
    }
    this.mbPerSec = mbPerSec;
    // NOTE: Double.POSITIVE_INFINITY casts to Long.MAX_VALUE
    minPauseCheckBytes = Math.min(1024*1024, (long) ((MIN_PAUSE_CHECK_MSEC / 1000.0) * mbPerSec * 1024 * 1024));
    assert minPauseCheckBytes >= 0;
    notify();
  }

  @Override
  public synchronized double getMBPerSec() {
    return mbPerSec;
  }

  /** Returns total bytes written by this merge. */
  public long getTotalBytesWritten() {
    return totalBytesWritten;
  }

  @Override
  public long pause(long bytes) throws MergePolicy.MergeAbortedException {

    totalBytesWritten += bytes;

    long startNS = System.nanoTime();
    long curNS = startNS;

    // While loop because 1) Thread.wait doesn't always sleep long
    // enough, and 2) we wake up and check again when our rate limit
    // is changed while we were pausing:
    long pausedNS = 0;
    while (true) {
      PauseResult result = maybePause(bytes, curNS);
      if (result == PauseResult.NO) {
        // Set to curNS, not targetNS, to enforce the instant rate, not
        // the "averaaged over all history" rate:
        lastNS = curNS;
        break;
      }
      curNS = System.nanoTime();
      long ns = curNS - startNS;
      startNS = curNS;

      // Separately track when merge was stopped vs rate limited:
      if (result == PauseResult.STOPPED) {
        totalStoppedNS += ns;
      } else {
        assert result == PauseResult.PAUSED;
        totalPausedNS += ns;
      }
      pausedNS += ns;
    }

    return pausedNS;
  }

  /** Total NS merge was stopped. */
  public synchronized long getTotalStoppedNS() {
    return totalStoppedNS;
  } 

  /** Total NS merge was paused to rate limit IO. */
  public synchronized long getTotalPausedNS() {
    return totalPausedNS;
  } 

  /** Returns NO if no pause happened, STOPPED if pause because rate was 0.0 (merge is stopped), PAUSED if paused with a normal rate limit. */
  private synchronized PauseResult maybePause(long bytes, long curNS) throws MergePolicy.MergeAbortedException {

    // Now is a good time to abort the merge:
    checkAbort();

    double secondsToPause = (bytes/1024./1024.) / mbPerSec;

    // Time we should sleep until; this is purely instantaneous
    // rate (just adds seconds onto the last time we had paused to);
    // maybe we should also offer decayed recent history one?
    long targetNS = lastNS + (long) (1000000000 * secondsToPause);

    long curPauseNS = targetNS - curNS;

    // NOTE: except maybe on real-time JVMs, minimum realistic
    // wait/sleep time is 1 msec; if you pass just 1 nsec the impl
    // rounds up to 1 msec, so we don't bother unless it's > 2 msec:

    if (curPauseNS <= 2000000) {
      return PauseResult.NO;
    }

    // Defensive: sleep for at most 250 msec; the loop above will call us again if we should keep sleeping:
    if (curPauseNS > 250L*1000000) {
      curPauseNS = 250L*1000000;
    }

    int sleepMS = (int) (curPauseNS / 1000000);
    int sleepNS = (int) (curPauseNS % 1000000);

    double rate = mbPerSec;

    try {
      // CMS can wake us up here if it changes our target rate:
      wait(sleepMS, sleepNS);
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }

    if (rate == 0.0) {
      return PauseResult.STOPPED;
    } else {
      return PauseResult.PAUSED;
    }
  }

  /** Throws {@link MergePolicy.MergeAbortedException} if this merge was aborted. */
  public synchronized void checkAbort() throws MergePolicy.MergeAbortedException {
    if (abort) {
      throw new MergePolicy.MergeAbortedException("merge is aborted: " + merge.segString());
    }
  }

  /** Mark this merge aborted. */
  public synchronized void setAbort() {
    abort = true;
    notify();
  }

  /** Returns true if this merge was aborted. */
  public synchronized boolean getAbort() {
    return abort;
  }

  @Override
  public long getMinPauseCheckBytes() {
    return minPauseCheckBytes;
  }
}
