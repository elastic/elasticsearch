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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * This class holds the timing breakdown for a single "node" in the query tree.  A node's
 * time may be composed of several internal attributes (rewriting, weighting, scoring, etc).
 *
 * This class holds the total accumulated time in the `timings` array, and uses the `scratch`
 * array to hold temporary data.  This is necessary, since a single node may start a timing
 * in one context (e.g. normalize), then start a new time under a different context (weight).
 * Since both of these timings are still "open", they need to be stored separately and able
 * to close independently
 *
 * This wrapper class is also serializable and ToXContent, so it is used directly in the
 * response element
 */
public class InternalProfileBreakdown implements ProfileBreakdown, Streamable, ToXContent {

    /**
     * The accumulated timings for this query node
     */
    private long[] timings;

    /**
     * The temporary scratch space for holding start-times
     */
    private long[] scratch;

    public InternalProfileBreakdown() {
        timings = new long[TimingType.values().length];
        scratch = new long[TimingType.values().length];
    }

    /**
     * Begin timing a query for a specific Timing context
     * @param timing    The timing context being profiled
     */
    public void startTime(TimingType timing) {
        scratch[timing.ordinal()] = System.nanoTime();
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
        long time = System.nanoTime();

        time = time - scratch[timing.ordinal()];

        timings[timing.ordinal()] += time;
        scratch[timing.ordinal()] = 0L;
        return time;
    }

    /**
     * Overwrites the accumulated time for a specific Timing context.
     * Used predominantly when deserializing a Timing from another node
     * @param type  The Timing context to set the time for
     * @param time  The accumulated time to overwrite with
     */
    public void setTime(TimingType type, long time) {
        timings[type.ordinal()] = time;
    }

    /**
     * Returns the accumulated time for a specific Timing context
     * @param type  The Timing context to retrieve
     * @return      The accumulated time
     */
    public long getTime(TimingType type) {
        return timings[type.ordinal()];
    }

    /**
     * Returns the total time for this query node (e.g. the summation of
     * all the Timing contexts)
     *
     * @return The total accumulated time
     */
    public long getTotalTime() {
        long time = 0;
        for (TimingType type : TimingType.values()) {
            time += timings[type.ordinal()];
        }
        return time;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timings = in.readLongArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLongArray(timings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (TimingType type : TimingType.values()) {
            builder = builder.field(type.toString(), timings[type.ordinal()]);
        }
        return builder;
    }
}
