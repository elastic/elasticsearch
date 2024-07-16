/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.admin.cluster.stats.LongMetric.LongMetricValue;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Holds a snapshot of the CCS telemetry statistics.
 * Used to hold the stats for a single node that's part of a {@link ClusterStatsNodeResponse}, as well as to
 * accumulate stats for the entire cluster and return them as part of the {@link ClusterStatsResponse}.
 */
public final class CCSTelemetrySnapshot implements Writeable, ToXContentFragment {
    private long totalCount;
    private long successCount;
    private final Map<String, Long> failureReasons;

    private final LongMetricValue took;
    private final LongMetricValue tookMrtTrue;
    private final LongMetricValue tookMrtFalse;
    private long remotesPerSearchMax;
    private double remotesPerSearchAvg;
    private long skippedRemotes;

    private final Map<String, Long> featureCounts;

    private final Map<String, Long> clientCounts;
    private final Map<String, PerClusterCCSTelemetry> byRemoteCluster;

    public long getTotalCount() {
        return totalCount;
    }

    public long getSuccessCount() {
        return successCount;
    }

    public Map<String, Long> getFailureReasons() {
        return failureReasons;
    }

    public LongMetricValue getTook() {
        return took;
    }

    public LongMetricValue getTookMrtTrue() {
        return tookMrtTrue;
    }

    public LongMetricValue getTookMrtFalse() {
        return tookMrtFalse;
    }

    public long getRemotesPerSearchMax() {
        return remotesPerSearchMax;
    }

    public double getRemotesPerSearchAvg() {
        return remotesPerSearchAvg;
    }

    public long getSkippedRemotes() {
        return skippedRemotes;
    }

    public Map<String, Long> getFeatureCounts() {
        return featureCounts;
    }

    public Map<String, Long> getClientCounts() {
        return clientCounts;
    }

    public Map<String, PerClusterCCSTelemetry> getByRemoteCluster() {
        return byRemoteCluster;
    }

    public static class PerClusterCCSTelemetry implements Writeable, ToXContentFragment {
        private long count;
        private long skippedCount;
        private final LongMetricValue took;

        public PerClusterCCSTelemetry() {
            took = new LongMetricValue();
        }

        public PerClusterCCSTelemetry(long count, long skippedCount, LongMetricValue took) {
            this.took = took;
            this.skippedCount = skippedCount;
            this.count = count;
        }

        public PerClusterCCSTelemetry(StreamInput in) throws IOException {
            this.count = in.readVLong();
            this.skippedCount = in.readVLong();
            this.took = LongMetricValue.fromStream(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(count);
            out.writeVLong(skippedCount);
            took.writeTo(out);
        }

        public PerClusterCCSTelemetry add(PerClusterCCSTelemetry v) {
            count += v.count;
            skippedCount += v.skippedCount;
            took.add(v.took);
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("total", count);
            builder.field("skipped", skippedCount);
            publishLatency(builder, took, "took");
            return builder;
        }

        public long getCount() {
            return count;
        }

        public long getSkippedCount() {
            return skippedCount;
        }

        public LongMetricValue getTook() {
            return took;
        }
    }

    /**
    * Creates a new stats instance with the provided info.
    */
    public CCSTelemetrySnapshot(
        long totalCount,
        long successCount,
        Map<String, Long> failureReasons,
        LongMetricValue took,
        LongMetricValue tookMrtTrue,
        LongMetricValue tookMrtFalse,
        long remotesPerSearchMax,
        double remotesPerSearchAvg,
        long skippedRemotes,
        Map<String, Long> featureCounts,
        Map<String, Long> clientCounts,
        Map<String, PerClusterCCSTelemetry> byRemoteCluster
    ) {
        this.totalCount = totalCount;
        this.successCount = successCount;
        this.failureReasons = failureReasons;
        this.took = took;
        this.tookMrtTrue = tookMrtTrue;
        this.tookMrtFalse = tookMrtFalse;
        this.remotesPerSearchMax = remotesPerSearchMax;
        this.remotesPerSearchAvg = remotesPerSearchAvg;
        this.skippedRemotes = skippedRemotes;
        this.featureCounts = featureCounts;
        this.clientCounts = clientCounts;
        this.byRemoteCluster = byRemoteCluster;
    }

    /**
     * Creates a new empty stats instance, that will get additional stats added through {@link #add(CCSTelemetrySnapshot)}
     */
    public CCSTelemetrySnapshot() {
        failureReasons = new HashMap<>();
        featureCounts = new HashMap<>();
        clientCounts = new HashMap<>();
        byRemoteCluster = new HashMap<>();
        took = new LongMetricValue();
        tookMrtTrue = new LongMetricValue();
        tookMrtFalse = new LongMetricValue();
    }

    public CCSTelemetrySnapshot(StreamInput in) throws IOException {
        this.totalCount = in.readVLong();
        this.successCount = in.readVLong();
        this.failureReasons = in.readMap(StreamInput::readLong);
        this.took = LongMetricValue.fromStream(in);
        this.tookMrtTrue = LongMetricValue.fromStream(in);
        this.tookMrtFalse = LongMetricValue.fromStream(in);
        this.remotesPerSearchMax = in.readVLong();
        this.remotesPerSearchAvg = in.readDouble();
        this.skippedRemotes = in.readVLong();
        this.featureCounts = in.readMap(StreamInput::readLong);
        this.clientCounts = in.readMap(StreamInput::readLong);
        this.byRemoteCluster = in.readMap(PerClusterCCSTelemetry::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalCount);
        out.writeVLong(successCount);
        out.writeMap(failureReasons, StreamOutput::writeLong);
        took.writeTo(out);
        tookMrtTrue.writeTo(out);
        tookMrtFalse.writeTo(out);
        out.writeVLong(remotesPerSearchMax);
        out.writeDouble(remotesPerSearchAvg);
        out.writeVLong(skippedRemotes);
        out.writeMap(featureCounts, StreamOutput::writeLong);
        out.writeMap(clientCounts, StreamOutput::writeLong);
        out.writeMap(byRemoteCluster, StreamOutput::writeWriteable);
    }

    /**
     * Add the provided stats to the ones held by the current instance, effectively merging the two
     */
    public void add(CCSTelemetrySnapshot stats) {
        long oldCount = totalCount;
        totalCount += stats.totalCount;
        successCount += stats.successCount;
        skippedRemotes += stats.skippedRemotes;
        stats.failureReasons.forEach((k, v) -> failureReasons.merge(k, v, Long::sum));
        stats.featureCounts.forEach((k, v) -> featureCounts.merge(k, v, Long::sum));
        stats.clientCounts.forEach((k, v) -> clientCounts.merge(k, v, Long::sum));
        took.add(stats.took);
        tookMrtTrue.add(stats.tookMrtTrue);
        tookMrtFalse.add(stats.tookMrtFalse);
        remotesPerSearchMax = Math.max(remotesPerSearchMax, stats.remotesPerSearchMax);
        // Weighted average
        remotesPerSearchAvg = (double) (remotesPerSearchMax * oldCount + stats.remotesPerSearchMax * stats.totalCount) / totalCount;
        stats.byRemoteCluster.forEach((r, v) -> byRemoteCluster.merge(r, v, PerClusterCCSTelemetry::add));
    }

    /**
     * Publishes the latency statistics to the provided {@link XContentBuilder}.
     * Example:
     * "took": {
     *      "max": 345032,
     *      "avg": 1620,
     *      "p90": 2570
     * }
     */
    public static void publishLatency(XContentBuilder builder, LongMetricValue took, String name) throws IOException {
        builder.startObject(name);
        {
            builder.field("max", took.max());
            builder.field("avg", took.avg());
            builder.field("p90", took.p90());
        }
        builder.endObject();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("ccs_telemetry");
        {
            builder.field("total", totalCount);
            builder.field("success", successCount);
            builder.field("skipped", skippedRemotes);
            publishLatency(builder, took, "took");
            publishLatency(builder, tookMrtTrue, "took_mrt_true");
            publishLatency(builder, tookMrtFalse, "took_mrt_false");
            builder.field("remotes_per_search_max", remotesPerSearchMax);
            builder.field("remotes_per_search_avg", remotesPerSearchAvg);
            builder.field("failure_reasons", failureReasons);
            builder.field("feature_counts", featureCounts);
            builder.field("client_counts", clientCounts);
            builder.startObject("remote_clusters");
            {
                builder.field("count", byRemoteCluster.size());
                byRemoteCluster.forEach((name, clusterData) -> {
                    try {
                        builder.startObject(name);
                        clusterData.toXContent(builder, params);
                        builder.endObject();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CCSTelemetrySnapshot that = (CCSTelemetrySnapshot) o;
        return totalCount == that.totalCount
            && successCount == that.successCount
            && skippedRemotes == that.skippedRemotes
            && Objects.equals(failureReasons, that.failureReasons)
            && Objects.equals(took, that.took)
            && Objects.equals(tookMrtTrue, that.tookMrtTrue)
            && Objects.equals(tookMrtFalse, that.tookMrtFalse)
            && Objects.equals(remotesPerSearchMax, that.remotesPerSearchMax)
            && Objects.equals(remotesPerSearchAvg, that.remotesPerSearchAvg)
            && Objects.equals(featureCounts, that.featureCounts)
            && Objects.equals(clientCounts, that.clientCounts)
            && Objects.equals(byRemoteCluster, that.byRemoteCluster);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            totalCount,
            successCount,
            failureReasons,
            took,
            tookMrtTrue,
            tookMrtFalse,
            remotesPerSearchMax,
            remotesPerSearchAvg,
            skippedRemotes,
            featureCounts,
            clientCounts,
            byRemoteCluster
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
