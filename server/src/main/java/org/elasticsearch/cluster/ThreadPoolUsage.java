/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

/**
 * A collection of {@link Sample}s on a specific Node and ThreadPool.
 */
public record ThreadPoolUsage(List<Sample> samples) implements Writeable {

    static ThreadPoolUsage fromStreamInput(StreamInput in) throws IOException {
        return new ThreadPoolUsage(in.readCollectionAsImmutableList(Sample::fromStreamInput));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(samples);
    }

    /**
     * Record of usage stats for a thread pool.
     *
     * @param timeNano            Sample time from System.nanoTime()
     * @param totalThreads        Total number of threads in the thread pool.
     * @param averageUtilization  Percent of thread pool threads that are in use, averaged over some period of time.
     * @param averageQueueLatency How much time tasks spend in the thread pool queue. Zero if there is nothing being queued in the write
     *                            thread pool.
     */
    public record Sample(long timeNano, int totalThreads, float averageUtilization, long averageQueueLatency) implements Writeable {

        public static Sample fromStreamInput(StreamInput in) throws IOException {
            return new Sample(in.readVLong(), in.readVInt(), in.readFloat(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(timeNano);
            out.writeVInt(totalThreads);
            out.writeFloat(averageUtilization);
            out.writeVLong(averageQueueLatency);
        }
    }
}
