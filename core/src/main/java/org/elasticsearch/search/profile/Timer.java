/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
// TODO: do not time every single call as discussed in https://github.com/elastic/elasticsearch/issues/24799
public final class Timer {

    private long timing, count, start;

    /** Start the timer. */
    public void start() {
        assert start == 0 : "#start call misses a matching #stop call";
        count++;
        start = System.nanoTime();
    }

    /** Stop the timer. */
    public void stop() {
        timing += Math.max(System.nanoTime() - start, 1L);
        start = 0;
    }

    /** Return the number of times that {@link #start()} has been called. */
    public long getCount() {
        if (start != 0) {
            throw new IllegalStateException("#start call misses a matching #stop call");
        }
        return count;
    }

    /** Return an approximation of the total time spend between consecutive calls of #start and #stop. */
    public long getTiming() {
        if (start != 0) {
            throw new IllegalStateException("#start call misses a matching #stop call");
        }
        return timing;
    }

}
