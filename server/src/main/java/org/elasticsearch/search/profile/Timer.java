/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

/** Helps measure how much time is spent running some methods.
 *  The {@link #start()} and {@link #stop()} methods should typically be called
 *  in a try/finally clause with {@link #start()} being called right before the
 *  try block and {@link #stop()} being called at the beginning of the finally
 *  block:
 *  <pre>
 *  timer.start();
 *  try {
 *    // code to time
 *  } finally {
 *    timer.stop();
 *  }
 *  </pre>
 */
public class Timer {

    private boolean doTiming;
    private long timing, count, lastCount, start;

    /** pkg-private for testing */
    long nanoTime() {
        return System.nanoTime();
    }

    /** Start the timer. */
    public final void start() {
        assert start == 0 : "#start call misses a matching #stop call";
        // We measure the timing of each method call for the first 256
        // calls, then 1/2 call up to 512 then 1/3 up to 768, etc. with
        // a maximum interval of 1024, which is reached for 1024*2^8 ~= 262000
        // This allows to not slow down things too much because of calls
        // to System.nanoTime() when methods are called millions of time
        // in tight loops, while still providing useful timings for methods
        // that are only called a couple times per search execution.
        doTiming = (count - lastCount) >= Math.min(lastCount >>> 8, 1024);
        if (doTiming) {
            start = nanoTime();
        }
        count++;
    }

    /** Stop the timer. */
    public final void stop() {
        if (doTiming) {
            timing += (count - lastCount) * Math.max(nanoTime() - start, 1L);
            lastCount = count;
            start = 0;
        }
    }

    /** Return the number of times that {@link #start()} has been called. */
    public final long getCount() {
        if (start != 0) {
            throw new IllegalStateException("#start call misses a matching #stop call");
        }
        return count;
    }

    /** Return an approximation of the total time spent between consecutive calls of #start and #stop. */
    public final long getApproximateTiming() {
        if (start != 0) {
            throw new IllegalStateException("#start call misses a matching #stop call");
        }
        // We don't have timings for the last `count-lastCount` method calls
        // so we assume that they had the same timing as the lastCount first
        // calls. This approximation is ok since at most 1/256th of method
        // calls have not been timed.
        long timing = this.timing;
        if (count > lastCount) {
            assert lastCount > 0;
            timing += (count - lastCount) * timing / lastCount;
        }
        return timing;
    }
}
