/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

/**
 * Record of a node's thread pool usage stats (operation load). Maps thread pool stats by thread pool name.
 *
 * @param nodeId The node ID.
 * @param threadPoolUsageStatsMap A map of thread pool name ({@link org.elasticsearch.threadpool.ThreadPool.Names}) to the thread pool's
 *                                usage stats ({@link ThreadPoolUsageStats}).
 */
public record NodeUsageStatsForThreadPools(String nodeId, Map<String, ThreadPoolUsageStats> threadPoolUsageStatsMap, Instant timestamp)
    implements
        Writeable {

    public static NodeUsageStatsForThreadPools readFrom(StreamInput in) throws IOException {
        final var nodeId = in.readString();
        final var threadPoolUsageStatsMap = in.readImmutableMap(ThreadPoolUsageStats::new);
        final Instant receivedTime;
        if (in.getTransportVersion().onOrAfter(TransportVersions.TIMESTAMP_IN_NODE_USAGE_STATS_FOR_THREAD_POOLS)) {
            receivedTime = in.readInstant();
        } else {
            receivedTime = Instant.now();
        }
        return new NodeUsageStatsForThreadPools(nodeId, threadPoolUsageStatsMap, receivedTime);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeMap(this.threadPoolUsageStatsMap, StreamOutput::writeWriteable);
        if (out.getTransportVersion().onOrAfter(TransportVersions.TIMESTAMP_IN_NODE_USAGE_STATS_FOR_THREAD_POOLS)) {
            out.writeInstant(this.timestamp);
        }
    }

    /**
     * Record of usage stats for a thread pool.
     *
     * @param totalThreadPoolThreads Total number of threads in the thread pool.
     * @param averageThreadPoolUtilization Percent of thread pool threads that are in use, averaged over some period of time.
     * @param maxThreadPoolQueueLatencyMillis The max time any task has spent in the thread pool queue. Zero if no task is queued.
     */
    public record ThreadPoolUsageStats(int totalThreadPoolThreads, float averageThreadPoolUtilization, long maxThreadPoolQueueLatencyMillis)
        implements
            Writeable {

        public ThreadPoolUsageStats(StreamInput in) throws IOException {
            this(in.readVInt(), in.readFloat(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.totalThreadPoolThreads);
            out.writeFloat(this.averageThreadPoolUtilization);
            out.writeVLong(this.maxThreadPoolQueueLatencyMillis);
        }
    }
}
