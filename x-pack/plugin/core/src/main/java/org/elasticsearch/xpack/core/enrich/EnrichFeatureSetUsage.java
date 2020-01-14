/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich;

import java.io.IOException;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Objects;
import java.util.stream.Collectors;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutionStats;

public class EnrichFeatureSetUsage extends XPackFeatureSet.Usage {

    private final ExecutionStats executionStats;
    private final CoordinatorSummaryStats coordinatorSummaryStats;

    public EnrichFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.executionStats = input.readOptionalWriteable(ExecutionStats::new);
        this.coordinatorSummaryStats = input.readOptionalWriteable(CoordinatorSummaryStats::new);
    }

    public EnrichFeatureSetUsage(boolean available, boolean enabled) {
        this(available, enabled, null, null);
    }

    public EnrichFeatureSetUsage(boolean available, boolean enabled, ExecutionStats executionStats,
                                 CoordinatorSummaryStats coordinatorSummaryStats) {
        super(XPackField.ENRICH, available, enabled);
        this.executionStats = executionStats;
        this.coordinatorSummaryStats = coordinatorSummaryStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(executionStats);
        out.writeOptionalWriteable(coordinatorSummaryStats);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (executionStats != null) {
            builder.startObject("execution_stats");
            executionStats.toXContent(builder, params);
            builder.endObject();
        }
        if (coordinatorSummaryStats != null) {
            builder.startObject("coordinator_stats");
            coordinatorSummaryStats.toXContent(builder, params);
            builder.endObject();
        }
    }

    public ExecutionStats getExecutionStats() {
        return executionStats;
    }

    public CoordinatorSummaryStats getCoordinatorSummaryStats() {
        return coordinatorSummaryStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichFeatureSetUsage that = (EnrichFeatureSetUsage) o;
        return Objects.equals(executionStats, that.executionStats) &&
            Objects.equals(coordinatorSummaryStats, that.coordinatorSummaryStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionStats, coordinatorSummaryStats);
    }

    public static class CoordinatorSummaryStats implements Writeable, ToXContentFragment {

        public static CoordinatorSummaryStats aggregate(List<CoordinatorStats> coordinatorStats) {
            int coordinatorCount = coordinatorStats.size();

            IntSummaryStatistics queueSummary = coordinatorStats.stream().collect(
                Collectors.summarizingInt(CoordinatorStats::getQueueSize));
            long minQueueSize = queueSummary.getMin() == Integer.MAX_VALUE ? 0 : queueSummary.getMin();
            long maxQueueSize = queueSummary.getMax() == Integer.MIN_VALUE ? 0 : queueSummary.getMax();

            LongSummaryStatistics currentRemoteRequestSummary = coordinatorStats.stream().collect(
                Collectors.summarizingLong(CoordinatorStats::getRemoteRequestsCurrent));
            long minCurrentRemoteReqs = currentRemoteRequestSummary.getMin() == Long.MAX_VALUE ? 0 : currentRemoteRequestSummary.getMin();
            long maxCurrentRemoteReqs = currentRemoteRequestSummary.getMax() == Long.MIN_VALUE ? 0 : currentRemoteRequestSummary.getMax();

            LongSummaryStatistics remoteRequestSummary = coordinatorStats.stream().collect(
                Collectors.summarizingLong(CoordinatorStats::getRemoteRequestsTotal));
            long minRemoteRequests = remoteRequestSummary.getMin() == Long.MAX_VALUE ? 0 : remoteRequestSummary.getMin();
            long maxRemoteRequests = remoteRequestSummary.getMax() == Long.MIN_VALUE ? 0 : remoteRequestSummary.getMax();

            LongSummaryStatistics executedSearchesSummary = coordinatorStats.stream().collect(
                Collectors.summarizingLong(CoordinatorStats::getExecutedSearchesTotal));
            long minExecutedSearches = executedSearchesSummary.getMin() == Long.MAX_VALUE ? 0 : executedSearchesSummary.getMin();
            long maxExecutedSearches = executedSearchesSummary.getMax() == Long.MIN_VALUE ? 0 : executedSearchesSummary.getMax();

            return new CoordinatorSummaryStats(
                coordinatorCount,
                queueSummary.getSum(),
                Math.round(queueSummary.getAverage()),
                minQueueSize,
                maxQueueSize,
                currentRemoteRequestSummary.getSum(),
                Math.round(currentRemoteRequestSummary.getAverage()),
                minCurrentRemoteReqs,
                maxCurrentRemoteReqs,
                remoteRequestSummary.getSum(),
                Math.round(remoteRequestSummary.getAverage()),
                minRemoteRequests,
                maxRemoteRequests,
                executedSearchesSummary.getSum(),
                Math.round(executedSearchesSummary.getAverage()),
                minExecutedSearches,
                maxExecutedSearches);
        }

        private final int coordinatorCount;
        private final long totalQueueSize;
        private final long avgQueueSize;
        private final long minQueueSize;
        private final long maxQueueSize;
        private final long totalCurrentRemoteRequests;
        private final long avgCurrentRemoteRequests;
        private final long minCurrentRemoteRequests;
        private final long maxCurrentRemoteRequests;
        private final long totalRemoteRequests;
        private final long avgRemoteRequests;
        private final long minRemoteRequests;
        private final long maxRemoteRequests;
        private final long totalExecutedSearches;
        private final long avgExecutedSearches;
        private final long minExecutedSearches;
        private final long maxExecutedSearches;

        public CoordinatorSummaryStats(int coordinatorCount,
                                       long totalQueueSize,
                                       long avgQueueSize,
                                       long minQueueSize,
                                       long maxQueueSize,
                                       long totalCurrentRemoteRequests,
                                       long avgCurrentRemoteRequests,
                                       long minCurrentRemoteRequests,
                                       long maxCurrentRemoteRequests,
                                       long totalRemoteRequests,
                                       long avgRemoteRequests,
                                       long minRemoteRequests,
                                       long maxRemoteRequests,
                                       long totalExecutedSearches,
                                       long avgExecutedSearches,
                                       long minExecutedSearches,
                                       long maxExecutedSearches) {
            this.coordinatorCount = coordinatorCount;
            this.totalQueueSize = totalQueueSize;
            this.avgQueueSize = avgQueueSize;
            this.minQueueSize = minQueueSize;
            this.maxQueueSize = maxQueueSize;
            this.totalCurrentRemoteRequests = totalCurrentRemoteRequests;
            this.avgCurrentRemoteRequests = avgCurrentRemoteRequests;
            this.minCurrentRemoteRequests = minCurrentRemoteRequests;
            this.maxCurrentRemoteRequests = maxCurrentRemoteRequests;
            this.totalRemoteRequests = totalRemoteRequests;
            this.avgRemoteRequests = avgRemoteRequests;
            this.minRemoteRequests = minRemoteRequests;
            this.maxRemoteRequests = maxRemoteRequests;
            this.totalExecutedSearches = totalExecutedSearches;
            this.avgExecutedSearches = avgExecutedSearches;
            this.minExecutedSearches = minExecutedSearches;
            this.maxExecutedSearches = maxExecutedSearches;
        }

        public CoordinatorSummaryStats(StreamInput in) throws IOException {
            this(in.readVInt(),
                in.readZLong(), in.readZLong(), in.readZLong(), in.readZLong(),
                in.readZLong(), in.readZLong(), in.readZLong(), in.readZLong(),
                in.readZLong(), in.readZLong(), in.readZLong(), in.readZLong(),
                in.readZLong(), in.readZLong(), in.readZLong(), in.readZLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(coordinatorCount);
            out.writeZLong(totalQueueSize);
            out.writeZLong(avgQueueSize);
            out.writeZLong(minQueueSize);
            out.writeZLong(maxQueueSize);
            out.writeZLong(totalCurrentRemoteRequests);
            out.writeZLong(avgCurrentRemoteRequests);
            out.writeZLong(minCurrentRemoteRequests);
            out.writeZLong(maxCurrentRemoteRequests);
            out.writeZLong(totalRemoteRequests);
            out.writeZLong(avgRemoteRequests);
            out.writeZLong(minRemoteRequests);
            out.writeZLong(maxRemoteRequests);
            out.writeZLong(totalExecutedSearches);
            out.writeZLong(avgExecutedSearches);
            out.writeZLong(minExecutedSearches);
            out.writeZLong(maxExecutedSearches);
        }

        public int getCoordinatorCount() {
            return coordinatorCount;
        }

        public long getTotalQueueSize() {
            return totalQueueSize;
        }

        public long getAvgQueueSize() {
            return avgQueueSize;
        }

        public long getMinQueueSize() {
            return minQueueSize;
        }

        public long getMaxQueueSize() {
            return maxQueueSize;
        }

        public long getTotalCurrentRemoteRequests() {
            return totalCurrentRemoteRequests;
        }

        public long getAvgCurrentRemoteRequests() {
            return avgCurrentRemoteRequests;
        }

        public long getMinCurrentRemoteRequests() {
            return minCurrentRemoteRequests;
        }

        public long getMaxCurrentRemoteRequests() {
            return maxCurrentRemoteRequests;
        }

        public long getTotalRemoteRequests() {
            return totalRemoteRequests;
        }

        public long getAvgRemoteRequests() {
            return avgRemoteRequests;
        }

        public long getMinRemoteRequests() {
            return minRemoteRequests;
        }

        public long getMaxRemoteRequests() {
            return maxRemoteRequests;
        }

        public long getTotalExecutedSearches() {
            return totalExecutedSearches;
        }

        public long getAvgExecutedSearches() {
            return avgExecutedSearches;
        }

        public long getMinExecutedSearches() {
            return minExecutedSearches;
        }

        public long getMaxExecutedSearches() {
            return maxExecutedSearches;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("coordinator_count", coordinatorCount);
            builder.field("total_queue_size", totalQueueSize);
            builder.field("avg_queue_size", avgQueueSize);
            builder.field("min_queue_size", minQueueSize);
            builder.field("max_queue_size", maxQueueSize);
            builder.field("total_current_remote_requests", totalCurrentRemoteRequests);
            builder.field("avg_current_remote_requests", avgCurrentRemoteRequests);
            builder.field("min_current_remote_requests", minCurrentRemoteRequests);
            builder.field("max_current_remote_requests", maxCurrentRemoteRequests);
            builder.field("total_remote_requests", totalRemoteRequests);
            builder.field("avg_remote_requests", avgRemoteRequests);
            builder.field("min_remote_requests", minRemoteRequests);
            builder.field("max_remote_requests", maxRemoteRequests);
            builder.field("total_executed_searches", totalExecutedSearches);
            builder.field("avg_executed_searches", avgExecutedSearches);
            builder.field("min_executed_searches", minExecutedSearches);
            builder.field("max_executed_searches", maxExecutedSearches);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CoordinatorSummaryStats that = (CoordinatorSummaryStats) o;
            return coordinatorCount == that.coordinatorCount &&
                totalQueueSize == that.totalQueueSize &&
                avgQueueSize == that.avgQueueSize &&
                minQueueSize == that.minQueueSize &&
                maxQueueSize == that.maxQueueSize &&
                totalCurrentRemoteRequests == that.totalCurrentRemoteRequests &&
                avgCurrentRemoteRequests == that.avgCurrentRemoteRequests &&
                minCurrentRemoteRequests == that.minCurrentRemoteRequests &&
                maxCurrentRemoteRequests == that.maxCurrentRemoteRequests &&
                totalRemoteRequests == that.totalRemoteRequests &&
                avgRemoteRequests == that.avgRemoteRequests &&
                minRemoteRequests == that.minRemoteRequests &&
                maxRemoteRequests == that.maxRemoteRequests &&
                totalExecutedSearches == that.totalExecutedSearches &&
                avgExecutedSearches == that.avgExecutedSearches &&
                minExecutedSearches == that.minExecutedSearches &&
                maxExecutedSearches == that.maxExecutedSearches;
        }

        @Override
        public int hashCode() {
            return Objects.hash(coordinatorCount, totalQueueSize, avgQueueSize, minQueueSize, maxQueueSize, totalCurrentRemoteRequests,
                avgCurrentRemoteRequests, minCurrentRemoteRequests, maxCurrentRemoteRequests, totalRemoteRequests, avgRemoteRequests,
                minRemoteRequests, maxRemoteRequests, totalExecutedSearches, avgExecutedSearches, minExecutedSearches,
                maxExecutedSearches);
        }
    }
}
