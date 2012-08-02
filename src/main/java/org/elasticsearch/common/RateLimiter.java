/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common;

import org.elasticsearch.ElasticSearchInterruptedException;

/**
 */
// LUCENE MONITOR: Taken from trunk of Lucene at 06-09-11
public class RateLimiter {

    private volatile double nsPerByte;
    private volatile long lastNS;

    // TODO: we could also allow eg a sub class to dynamically
    // determine the allowed rate, eg if an app wants to
    // change the allowed rate over time or something

    /**
     * mbPerSec is the MB/sec max IO rate
     */
    public RateLimiter(double mbPerSec) {
        setMaxRate(mbPerSec);
    }

    public void setMaxRate(double mbPerSec) {
        nsPerByte = 1000000000. / (1024 * 1024 * mbPerSec);
    }

    /**
     * Pauses, if necessary, to keep the instantaneous IO
     * rate at or below the target. NOTE: multiple threads
     * may safely use this, however the implementation is
     * not perfectly thread safe but likely in practice this
     * is harmless (just means in some rare cases the rate
     * might exceed the target).  It's best to call this
     * with a biggish count, not one byte at a time.
     */
    public long pause(long bytes) {

        // TODO: this is purely instantenous rate; maybe we
        // should also offer decayed recent history one?
        final long targetNS = lastNS = lastNS + ((long) (bytes * nsPerByte));
        long curNS = System.nanoTime();
        if (lastNS < curNS) {
            lastNS = curNS;
        }

        // While loop because Thread.sleep doesn't alway sleep
        // enough:
        long totalPauseTime = 0;
        while (true) {
            final long pauseNS = targetNS - curNS;
            if (pauseNS > 0) {
                try {
                    Thread.sleep((int) (pauseNS / 1000000), (int) (pauseNS % 1000000));
                    totalPauseTime += pauseNS;
                } catch (InterruptedException ie) {
                    throw new ElasticSearchInterruptedException("interrupted while rate limiting", ie);
                }
                curNS = System.nanoTime();
                continue;
            }
            break;
        }
        return totalPauseTime;
    }
}
