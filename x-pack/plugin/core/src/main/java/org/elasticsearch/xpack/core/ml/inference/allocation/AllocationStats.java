/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
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
        private final RoutingStateAndReason routingState;
        private final Instant startTime;
        private final Integer inferenceThreads;
        private final Integer modelThreads;

        public static AllocationStats.NodeStats forStartedState(
            DiscoveryNode node,
            long inferenceCount,
            Double avgInferenceTime,
            int pendingCount,
            Instant lastAccess,
            Instant startTime,
            Integer inferenceThreads,
            Integer modelThreads
        ) {
            return new AllocationStats.NodeStats(
                node,
                inferenceCount,
                avgInferenceTime,
                lastAccess,
                pendingCount,
                new RoutingStateAndReason(RoutingState.STARTED, null),
                Objects.requireNonNull(startTime),
                inferenceThreads,
                modelThreads
            );
        }

        public static AllocationStats.NodeStats forNotStartedState(DiscoveryNode node, RoutingState state, String reason) {
            return new AllocationStats.NodeStats(node, null, null, null, null, new RoutingStateAndReason(state, reason), null, null, null);
        }

        public NodeStats(
            DiscoveryNode node,
            Long inferenceCount,
            Double avgInferenceTime,
            Instant lastAccess,
            Integer pendingCount,
            RoutingStateAndReason routingState,
            @Nullable Instant startTime,
            @Nullable Integer inferenceThreads,
            @Nullable Integer modelThreads
        ) {
            this.node = node;
            this.inferenceCount = inferenceCount;
            this.avgInferenceTime = avgInferenceTime;
            this.lastAccess = lastAccess;
            this.pendingCount = pendingCount;
            this.routingState = routingState;
            this.startTime = startTime;
            this.inferenceThreads = inferenceThreads;
            this.modelThreads = modelThreads;

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
            } else {
                this.inferenceThreads = null;
                this.modelThreads = null;
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

        public Instant getStartTime() {
            return startTime;
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
            if (avgInferenceTime != null) {
                builder.field("average_inference_time_ms", avgInferenceTime);
            }
            if (lastAccess != null) {
                builder.timeField("last_access", "last_access_string", lastAccess.toEpochMilli());
            }
            if (pendingCount != null) {
                builder.field("number_of_pending_requests", pendingCount);
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
                && Objects.equals(routingState, that.routingState)
                && Objects.equals(startTime, that.startTime)
                && Objects.equals(inferenceThreads, that.inferenceThreads)
                && Objects.equals(modelThreads, that.modelThreads);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                node,
                inferenceCount,
                avgInferenceTime,
                lastAccess,
                pendingCount,
                routingState,
                startTime,
                inferenceThreads,
                modelThreads
            );
        }
    }

    private final String modelId;
    private AllocationState state;
    private AllocationStatus allocationStatus;
    private String reason;
    @Nullable
    private final ByteSizeValue modelSize;
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
        @Nullable ByteSizeValue modelSize,
        @Nullable Integer inferenceThreads,
        @Nullable Integer modelThreads,
        @Nullable Integer queueCapacity,
        Instant startTime,
        List<AllocationStats.NodeStats> nodeStats
    ) {
        this.modelId = modelId;
        this.modelSize = modelSize;
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
        modelSize = in.readOptionalWriteable(ByteSizeValue::new);
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

    public ByteSizeValue getModelSize() {
        return modelSize;
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
        if (modelSize != null) {
            builder.humanReadableField("model_size_bytes", "model_size", modelSize);
        }
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
        out.writeOptionalWriteable(modelSize);
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
            && Objects.equals(modelSize, that.modelSize)
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
        return Objects.hash(
            modelId,
            modelSize,
            inferenceThreads,
            modelThreads,
            queueCapacity,
            startTime,
            nodeStats,
            state,
            reason,
            allocationStatus
        );
    }
}
