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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 */
public final class ProfileBreakdown {

    /** Enumeration of all supported timing types. */
    public enum TimingType {
        WEIGHT,
        BUILD_SCORER,
        NEXT_DOC,
        ADVANCE,
        MATCH,
        SCORE;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    /**
     * The accumulated timings for this query node
     */
    private final long[] timings;

    /**
     * The temporary scratch space for holding start-times
     */
    private long scratch;

    /** Sole constructor. */
    public ProfileBreakdown() {
        timings = new long[TimingType.values().length];
    }

    /**
     * Begin timing a query for a specific Timing context
     * @param timing    The timing context being profiled
     */
    public void startTime(TimingType timing) {
        assert scratch == 0;
        scratch = System.nanoTime();
    }

    /**
     * Halt the timing process and save the elapsed time.
     * startTime() must be called for a particular context prior to calling
     * stopAndRecordTime(), otherwise the elapsed time will be negative and
     * nonsensical
     *
     * @param timing    The timing context being profiled
     * @return          The elapsed time
     */
    public long stopAndRecordTime(TimingType timing) {
        long time = Math.max(1, System.nanoTime() - scratch);
        timings[timing.ordinal()] += time;
        scratch = 0L;
        return time;
    }

    /** Convert this record to a map from {@link TimingType} to times. */
    public Map<String, Long> toTimingMap() {
        Map<String, Long> map = new HashMap<>();
        for (TimingType timingType : TimingType.values()) {
            map.put(timingType.toString(), timings[timingType.ordinal()]);
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Add <code>other</code>'s timings into this breakdown
     * @param other Another Breakdown to merge with this one
     */
    public void merge(ProfileBreakdown other) {
        for (int i = 0; i < timings.length; ++i) {
            timings[i] += other.timings[i];
        }
    }
}
