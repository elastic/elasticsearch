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
import java.util.Map;
import java.util.Objects;

/**
 * Record of a node's thread pool usage stats (operation load). Maps thread pool stats by thread pool name.
 *
 * @param nodeId The node ID.
 * @param threadPoolUsageStatsMap A map of thread pool name ({@link org.elasticsearch.threadpool.ThreadPool.Names}) to the thread pool's
 *                                usage stats ({@link ThreadPoolUsageStats}).
 */
public record NodeUsageStatsForThreadPools(String nodeId, Map<String, ThreadPoolUsageStats> threadPoolUsageStatsMap) implements Writeable {

    public NodeUsageStatsForThreadPools(StreamInput in) throws IOException {
        this(in.readString(), in.readMap(ThreadPoolUsageStats::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeMap(threadPoolUsageStatsMap, StreamOutput::writeWriteable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, threadPoolUsageStatsMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeUsageStatsForThreadPools other = (NodeUsageStatsForThreadPools) o;
        for (var entry : other.threadPoolUsageStatsMap.entrySet()) {
            var loadStats = threadPoolUsageStatsMap.get(entry.getKey());
            if (loadStats == null || loadStats.equals(entry.getValue()) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(getClass().getSimpleName() + "{nodeId=" + nodeId + ", threadPoolUsageStatsMap=[");
        for (var entry : threadPoolUsageStatsMap.entrySet()) {
            builder.append("{ThreadPool.Names=" + entry.getKey() + ", ThreadPoolUsageStats=" + entry.getValue() + "}");
        }
        builder.append("]}");
        return builder.toString();
    }

    /**
     * Record of usage stats for a thread pool.
     *
     * @param totalThreadPoolThreads Total number of threads in the thread pool.
     * @param averageThreadPoolUtilization Percent of thread pool threads that are in use, averaged over some period of time.
     * @param averageThreadPoolQueueLatencyMillis How much time tasks spend in the thread pool queue. Zero if there is nothing being queued
     *                                            in the write thread pool.
     */
    public record ThreadPoolUsageStats(
        int totalThreadPoolThreads,
        float averageThreadPoolUtilization,
        long averageThreadPoolQueueLatencyMillis
    ) implements Writeable {

        public ThreadPoolUsageStats(StreamInput in) throws IOException {
            this(in.readVInt(), in.readFloat(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.totalThreadPoolThreads);
            out.writeFloat(this.averageThreadPoolUtilization);
            out.writeVLong(this.averageThreadPoolQueueLatencyMillis);
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalThreadPoolThreads, averageThreadPoolUtilization, averageThreadPoolQueueLatencyMillis);
        }

        @Override
        public String toString() {
            return "[totalThreadPoolThreads="
                + totalThreadPoolThreads
                + ", averageThreadPoolUtilization="
                + averageThreadPoolUtilization
                + ", averageThreadPoolQueueLatencyMillis="
                + averageThreadPoolQueueLatencyMillis
                + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ThreadPoolUsageStats other = (ThreadPoolUsageStats) o;
            return totalThreadPoolThreads == other.totalThreadPoolThreads
                && averageThreadPoolUtilization == other.averageThreadPoolUtilization
                && averageThreadPoolQueueLatencyMillis == other.averageThreadPoolQueueLatencyMillis;
        }

    } // ThreadPoolUsageStats

}
