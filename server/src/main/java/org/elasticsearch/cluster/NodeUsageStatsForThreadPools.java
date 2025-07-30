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
import java.util.List;
import java.util.Map;

/**
 * Record of a node's thread pool usage stats (operation load). Maps thread pool stats by thread pool name.
 *
 * @param nodeId The node ID.
 * @param threadPoolUsageStatsMap A map of thread pool name ({@link org.elasticsearch.threadpool.ThreadPool.Names}) to the thread pool's
 *                                usage stats ({@link ThreadPoolUsageStats}).
 */
public record NodeUsageStatsForThreadPools(String nodeId, Map<String, ThreadPoolUsageStats> threadPoolUsageStatsMap) implements Writeable {

    public NodeUsageStatsForThreadPools(StreamInput in) throws IOException {
        this(in.readString(), in.readMap(ThreadPoolUsageStats::readFrom));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeMap(threadPoolUsageStatsMap, StreamOutput::writeWriteable);
    }

    /**
     * One utilization sample
     *
     * @param instant The time we received the sample
     * @param utilization The utilization value in the range [0.0, 1.0]
     */
    public record UtilizationSample(Instant instant, float utilization) implements Writeable {

        public UtilizationSample(StreamInput in) throws IOException {
            this(Instant.ofEpochMilli(in.readVLong()), in.readFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(instant.toEpochMilli());
            out.writeFloat(utilization);
        }
    }

    /**
     * Record of usage stats for a thread pool.
     *
     * @param numberOfThreads Total number of threads in the thread pool.
     * @param utilizationSamples The list of recent utilization samples
     */
    public record ThreadPoolUsageStats(int numberOfThreads, List<UtilizationSample> utilizationSamples) implements Writeable {

        public ThreadPoolUsageStats {
            assert numberOfThreads > 0;
            assert utilizationSamples.isEmpty() == false;
        }

        public static ThreadPoolUsageStats readFrom(StreamInput in) throws IOException {
            final int numberOfThreads = in.readVInt();
            final List<UtilizationSample> utilizationSamples;
            if (in.getTransportVersion().onOrAfter(TransportVersions.THREAD_POOL_UTILIZATION_MULTI_SAMPLE_CLUSTER_INFO)) {
                utilizationSamples = in.readCollectionAsImmutableList(UtilizationSample::new);
            } else {
                utilizationSamples = List.of(new UtilizationSample(Instant.now(), in.readFloat()));
                in.readVLong(); // Skip over the queue latency
            }
            return new ThreadPoolUsageStats(numberOfThreads, utilizationSamples);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.numberOfThreads);
            if (out.getTransportVersion().onOrAfter(TransportVersions.THREAD_POOL_UTILIZATION_MULTI_SAMPLE_CLUSTER_INFO)) {
                out.writeGenericList(utilizationSamples, StreamOutput::writeWriteable);
            } else {
                out.writeFloat(this.utilizationSamples.getLast().utilization());
                out.writeVLong(0L); // Dummy queue latency value
            }
        }
    } // ThreadPoolUsageStats

}
