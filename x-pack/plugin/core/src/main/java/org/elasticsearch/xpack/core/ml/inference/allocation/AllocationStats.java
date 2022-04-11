/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class AllocationStats implements ToXContentObject, Writeable {

    public static class NodeStats implements ToXContentObject, Writeable {
        private final DiscoveryNode node;
        private final Long inferenceCount;
        private final Double avgInferenceTime;
        private final Instant lastAccess;
        private final Integer pendingCount;
        private final int errorCount;
        private final int rejectedExecutionCount;
        private final int timeoutCount;
        private final RoutingStateAndReason routingState;
        private final Instant startTime;
        private final Integer inferenceThreads;
        private final Integer modelThreads;
        private final long peakThroughput;
        private final long throughputLastPeriod;
        private final Double avgInferenceTimeLastPeriod;

        public static AllocationStats.NodeStats forStartedState(
            DiscoveryNode node,
            long inferenceCount,
            Double avgInferenceTime,
            int pendingCount,
            int errorCount,
            int rejectedExecutionCount,
            int timeoutCount,
            Instant lastAccess,
            Instant startTime,
            Integer inferenceThreads,
            Integer modelThreads,
            long peakThroughput,
            long throughputLastPeriod,
            Double avgInferenceTimeLastPeriod
        ) {
            return new AllocationStats.NodeStats(
                node,
                inferenceCount,
                avgInferenceTime,
                lastAccess,
                pendingCount,
                errorCount,
                rejectedExecutionCount,
                timeoutCount,
                new RoutingStateAndReason(RoutingState.STARTED, null),
                Objects.requireNonNull(startTime),
                inferenceThreads,
                modelThreads,
                peakThroughput,
                throughputLastPeriod,
                avgInferenceTimeLastPeriod
            );
        }

        public static AllocationStats.NodeStats forNotStartedState(DiscoveryNode node, RoutingState state, String reason) {
            return new AllocationStats.NodeStats(
                node,
                null,
                null,
                null,
                null,
                0,
                0,
                0,
                new RoutingStateAndReason(state, reason),
                null,
                null,
                null,
                0,
                0,
                null
            );
        }

        public NodeStats(
            DiscoveryNode node,
            Long inferenceCount,
            Double avgInferenceTime,
            @Nullable Instant lastAccess,
            Integer pendingCount,
            int errorCount,
            int rejectedExecutionCount,
            int timeoutCount,
            RoutingStateAndReason routingState,
            @Nullable Instant startTime,
            @Nullable Integer inferenceThreads,
            @Nullable Integer modelThreads,
            long peakThroughput,
            long throughputLastPeriod,
            Double avgInferenceTimeLastPeriod
        ) {
            this.node = node;
            this.inferenceCount = inferenceCount;
            this.avgInferenceTime = avgInferenceTime;
            this.lastAccess = lastAccess;
            this.pendingCount = pendingCount;
            this.errorCount = errorCount;
            this.rejectedExecutionCount = rejectedExecutionCount;
            this.timeoutCount = timeoutCount;
            this.routingState = routingState;
            this.startTime = startTime;
            this.inferenceThreads = inferenceThreads;
            this.modelThreads = modelThreads;
            this.peakThroughput = peakThroughput;
            this.throughputLastPeriod = throughputLastPeriod;
            this.avgInferenceTimeLastPeriod = avgInferenceTimeLastPeriod;

            // if lastAccess time is null there have been no inferences
            assert this.lastAccess != null || (inferenceCount == null || inferenceCount == 0);
        }

        public NodeStats(StreamInput in) throws IOException {
            this.node = in.readOptionalWriteable(DiscoveryNode::new);
            this.inferenceCount = in.readOptionalLong();
            this.avgInferenceTime = in.readOptionalDouble();
            this.lastAccess = in.readOptionalInstant();
            this.pendingCount = in.readOptionalVInt();
            this.routingState = in.readOptionalWriteable(RoutingStateAndReason::new);
            this.startTime = in.readOptionalInstant();
            if (in.getVersion().onOrAfter(Version.V_8_1_0)) {
                this.inferenceThreads = in.readOptionalVInt();
                this.modelThreads = in.readOptionalVInt();
                this.errorCount = in.readVInt();
                this.rejectedExecutionCount = in.readVInt();
                this.timeoutCount = in.readVInt();
            } else {
                this.inferenceThreads = null;
                this.modelThreads = null;
                this.errorCount = 0;
                this.rejectedExecutionCount = 0;
                this.timeoutCount = 0;
            }
            if (in.getVersion().onOrAfter(Version.V_8_2_0)) {
                this.peakThroughput = in.readVLong();
                this.throughputLastPeriod = in.readVLong();
                this.avgInferenceTimeLastPeriod = in.readOptionalDouble();
            } else {
                this.peakThroughput = 0;
                this.throughputLastPeriod = 0;
                this.avgInferenceTimeLastPeriod = null;
            }
        }

        public DiscoveryNode getNode() {
            return node;
        }

        public RoutingStateAndReason getRoutingState() {
            return routingState;
        }

        public Optional<Long> getInferenceCount() {
            return Optional.ofNullable(inferenceCount);
        }

        public Optional<Double> getAvgInferenceTime() {
            return Optional.ofNullable(avgInferenceTime);
        }

        public Instant getLastAccess() {
            return lastAccess;
        }

        public Integer getPendingCount() {
            return pendingCount;
        }

        public int getErrorCount() {
            return errorCount;
        }

        public int getRejectedExecutionCount() {
            return rejectedExecutionCount;
        }

        public int getTimeoutCount() {
            return timeoutCount;
        }

        public Instant getStartTime() {
            return startTime;
        }

        public Integer getInferenceThreads() {
            return inferenceThreads;
        }

        public Integer getModelThreads() {
            return modelThreads;
        }

        public long getPeakThroughput() {
            return peakThroughput;
        }

        public long getThroughputLastPeriod() {
            return throughputLastPeriod;
        }

        public Double getAvgInferenceTimeLastPeriod() {
            return avgInferenceTimeLastPeriod;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (node != null) {
                builder.startObject("node");
                node.toXContent(builder, params);
                builder.endObject();
            }
            builder.field("routing_state", routingState);
            if (inferenceCount != null) {
                builder.field("inference_count", inferenceCount);
            }
            // avoid reporting the average time as 0 if count < 1
            if (avgInferenceTime != null && (inferenceCount != null && inferenceCount > 0)) {
                builder.field("average_inference_time_ms", avgInferenceTime);
            }
            if (lastAccess != null) {
                builder.timeField("last_access", "last_access_string", lastAccess.toEpochMilli());
            }
            if (pendingCount != null) {
                builder.field("number_of_pending_requests", pendingCount);
            }
            if (errorCount > 0) {
                builder.field("error_count", errorCount);
            }
            if (rejectedExecutionCount > 0) {
                builder.field("rejected_execution_count", rejectedExecutionCount);
            }
            if (timeoutCount > 0) {
                builder.field("timeout_count", timeoutCount);
            }
            if (startTime != null) {
                builder.timeField("start_time", "start_time_string", startTime.toEpochMilli());
            }
            if (inferenceThreads != null) {
                builder.field("inference_threads", inferenceThreads);
            }
            if (modelThreads != null) {
                builder.field("model_threads", modelThreads);
            }
            builder.field("peak_throughput_per_minute", peakThroughput);
            builder.field("throughput_last_minute", throughputLastPeriod);
            if (avgInferenceTimeLastPeriod != null) {
                builder.field("average_inference_time_ms_last_minute", avgInferenceTimeLastPeriod);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(node);
            out.writeOptionalLong(inferenceCount);
            out.writeOptionalDouble(avgInferenceTime);
            out.writeOptionalInstant(lastAccess);
            out.writeOptionalVInt(pendingCount);
            out.writeOptionalWriteable(routingState);
            out.writeOptionalInstant(startTime);
            if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
                out.writeOptionalVInt(inferenceThreads);
                out.writeOptionalVInt(modelThreads);
                out.writeVInt(errorCount);
                out.writeVInt(rejectedExecutionCount);
                out.writeVInt(timeoutCount);
            }
            if (out.getVersion().onOrAfter(Version.V_8_2_0)) {
                out.writeVLong(peakThroughput);
                out.writeVLong(throughputLastPeriod);
                out.writeOptionalDouble(avgInferenceTimeLastPeriod);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AllocationStats.NodeStats that = (AllocationStats.NodeStats) o;
            return Objects.equals(inferenceCount, that.inferenceCount)
                && Objects.equals(that.avgInferenceTime, avgInferenceTime)
                && Objects.equals(node, that.node)
                && Objects.equals(lastAccess, that.lastAccess)
                && Objects.equals(pendingCount, that.pendingCount)
                && Objects.equals(errorCount, that.errorCount)
                && Objects.equals(rejectedExecutionCount, that.rejectedExecutionCount)
                && Objects.equals(timeoutCount, that.timeoutCount)
                && Objects.equals(routingState, that.routingState)
                && Objects.equals(startTime, that.startTime)
                && Objects.equals(inferenceThreads, that.inferenceThreads)
                && Objects.equals(modelThreads, that.modelThreads)
                && Objects.equals(peakThroughput, that.peakThroughput)
                && Objects.equals(throughputLastPeriod, that.throughputLastPeriod)
                && Objects.equals(avgInferenceTimeLastPeriod, that.avgInferenceTimeLastPeriod);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                node,
                inferenceCount,
                avgInferenceTime,
                lastAccess,
                pendingCount,
                errorCount,
                rejectedExecutionCount,
                timeoutCount,
                routingState,
                startTime,
                inferenceThreads,
                modelThreads,
                peakThroughput,
                throughputLastPeriod,
                avgInferenceTimeLastPeriod
            );
        }
    }

    private final String modelId;
    private AllocationState state;
    private AllocationStatus allocationStatus;
    private String reason;
    @Nullable
    private final Integer inferenceThreads;
    @Nullable
    private final Integer modelThreads;
    @Nullable
    private final Integer queueCapacity;
    private final Instant startTime;
    private final List<AllocationStats.NodeStats> nodeStats;

    public AllocationStats(
        String modelId,
        @Nullable Integer inferenceThreads,
        @Nullable Integer modelThreads,
        @Nullable Integer queueCapacity,
        Instant startTime,
        List<AllocationStats.NodeStats> nodeStats
    ) {
        this.modelId = modelId;
        this.inferenceThreads = inferenceThreads;
        this.modelThreads = modelThreads;
        this.queueCapacity = queueCapacity;
        this.startTime = Objects.requireNonNull(startTime);
        this.nodeStats = nodeStats;
        this.state = null;
        this.reason = null;
    }

    public AllocationStats(StreamInput in) throws IOException {
        modelId = in.readString();
        inferenceThreads = in.readOptionalVInt();
        modelThreads = in.readOptionalVInt();
        queueCapacity = in.readOptionalVInt();
        startTime = in.readInstant();
        nodeStats = in.readList(AllocationStats.NodeStats::new);
        state = in.readOptionalEnum(AllocationState.class);
        reason = in.readOptionalString();
        allocationStatus = in.readOptionalWriteable(AllocationStatus::new);
    }

    public String getModelId() {
        return modelId;
    }

    @Nullable
    public Integer getInferenceThreads() {
        return inferenceThreads;
    }

    @Nullable
    public Integer getModelThreads() {
        return modelThreads;
    }

    @Nullable
    public Integer getQueueCapacity() {
        return queueCapacity;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public List<AllocationStats.NodeStats> getNodeStats() {
        return nodeStats;
    }

    public AllocationState getState() {
        return state;
    }

    public AllocationStats setState(AllocationState state) {
        this.state = state;
        return this;
    }

    public AllocationStats setAllocationStatus(AllocationStatus allocationStatus) {
        this.allocationStatus = allocationStatus;
        return this;
    }

    public String getReason() {
        return reason;
    }

    public AllocationStats setReason(String reason) {
        this.reason = reason;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("model_id", modelId);
        if (inferenceThreads != null) {
            builder.field(StartTrainedModelDeploymentAction.TaskParams.INFERENCE_THREADS.getPreferredName(), inferenceThreads);
        }
        if (modelThreads != null) {
            builder.field(StartTrainedModelDeploymentAction.TaskParams.MODEL_THREADS.getPreferredName(), modelThreads);
        }
        if (queueCapacity != null) {
            builder.field(StartTrainedModelDeploymentAction.TaskParams.QUEUE_CAPACITY.getPreferredName(), queueCapacity);
        }
        if (state != null) {
            builder.field("state", state);
        }
        if (reason != null) {
            builder.field("reason", reason);
        }
        if (allocationStatus != null) {
            builder.field("allocation_status", allocationStatus);
        }
        builder.timeField("start_time", "start_time_string", startTime.toEpochMilli());

        int totalErrorCount = nodeStats.stream().mapToInt(NodeStats::getErrorCount).sum();
        int totalRejectedExecutionCount = nodeStats.stream().mapToInt(NodeStats::getRejectedExecutionCount).sum();
        int totalTimeoutCount = nodeStats.stream().mapToInt(NodeStats::getTimeoutCount).sum();
        long totalInferenceCount = nodeStats.stream()
            .filter(n -> n.getInferenceCount().isPresent())
            .mapToLong(n -> n.getInferenceCount().get())
            .sum();
        long peakThroughput = nodeStats.stream().mapToLong(NodeStats::getPeakThroughput).sum();

        if (totalErrorCount > 0) {
            builder.field("error_count", totalErrorCount);
        }
        if (totalRejectedExecutionCount > 0) {
            builder.field("rejected_execution_count", totalRejectedExecutionCount);
        }
        if (totalTimeoutCount > 0) {
            builder.field("timeout_count", totalTimeoutCount);
        }
        if (totalInferenceCount > 0) {
            builder.field("inference_count", totalInferenceCount);
        }
        builder.field("peak_throughput_per_minute", peakThroughput);

        builder.startArray("nodes");
        for (AllocationStats.NodeStats nodeStat : nodeStats) {
            nodeStat.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalVInt(inferenceThreads);
        out.writeOptionalVInt(modelThreads);
        out.writeOptionalVInt(queueCapacity);
        out.writeInstant(startTime);
        out.writeList(nodeStats);
        out.writeOptionalEnum(state);
        out.writeOptionalString(reason);
        out.writeOptionalWriteable(allocationStatus);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AllocationStats that = (AllocationStats) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(inferenceThreads, that.inferenceThreads)
            && Objects.equals(modelThreads, that.modelThreads)
            && Objects.equals(queueCapacity, that.queueCapacity)
            && Objects.equals(startTime, that.startTime)
            && Objects.equals(state, that.state)
            && Objects.equals(reason, that.reason)
            && Objects.equals(allocationStatus, that.allocationStatus)
            && Objects.equals(nodeStats, that.nodeStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, inferenceThreads, modelThreads, queueCapacity, startTime, nodeStats, state, reason, allocationStatus);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
