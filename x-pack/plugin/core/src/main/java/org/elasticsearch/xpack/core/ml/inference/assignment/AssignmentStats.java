/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class AssignmentStats implements ToXContentObject, Writeable {

    public static class NodeStats implements ToXContentObject, Writeable {
        private final DiscoveryNode node;
        private final Long inferenceCount;
        private final Double avgInferenceTime;
        private final Double avgInferenceTimeExcludingCacheHit;
        private final Instant lastAccess;
        private final Integer pendingCount;
        private final int errorCount;
        private final Long cacheHitCount;
        private final int rejectedExecutionCount;
        private final int timeoutCount;
        private final RoutingStateAndReason routingState;
        private final Instant startTime;
        private final Integer threadsPerAllocation;
        private final Integer numberOfAllocations;
        private final long peakThroughput;
        private final long throughputLastPeriod;
        private final Double avgInferenceTimeLastPeriod;
        private final Long cacheHitCountLastPeriod;

        public static AssignmentStats.NodeStats forStartedState(
            DiscoveryNode node,
            long inferenceCount,
            Double avgInferenceTime,
            Double avgInferenceTimeExcludingCacheHit,
            int pendingCount,
            int errorCount,
            long cacheHitCount,
            int rejectedExecutionCount,
            int timeoutCount,
            Instant lastAccess,
            Instant startTime,
            Integer threadsPerAllocation,
            Integer numberOfAllocations,
            long peakThroughput,
            long throughputLastPeriod,
            Double avgInferenceTimeLastPeriod,
            long cacheHitCountLastPeriod
        ) {
            return new AssignmentStats.NodeStats(
                node,
                inferenceCount,
                avgInferenceTime,
                avgInferenceTimeExcludingCacheHit,
                lastAccess,
                pendingCount,
                errorCount,
                cacheHitCount,
                rejectedExecutionCount,
                timeoutCount,
                new RoutingStateAndReason(RoutingState.STARTED, null),
                Objects.requireNonNull(startTime),
                threadsPerAllocation,
                numberOfAllocations,
                peakThroughput,
                throughputLastPeriod,
                avgInferenceTimeLastPeriod,
                cacheHitCountLastPeriod
            );
        }

        public static AssignmentStats.NodeStats forNotStartedState(DiscoveryNode node, RoutingState state, String reason) {
            return new AssignmentStats.NodeStats(
                node,
                null,
                null,
                null,
                null,
                null,
                0,
                null,
                0,
                0,
                new RoutingStateAndReason(state, reason),
                null,
                null,
                null,
                0L,
                0L,
                null,
                null
            );
        }

        public NodeStats(
            DiscoveryNode node,
            Long inferenceCount,
            Double avgInferenceTime,
            Double avgInferenceTimeExcludingCacheHit,
            @Nullable Instant lastAccess,
            Integer pendingCount,
            int errorCount,
            Long cacheHitCount,
            int rejectedExecutionCount,
            int timeoutCount,
            RoutingStateAndReason routingState,
            @Nullable Instant startTime,
            @Nullable Integer threadsPerAllocation,
            @Nullable Integer numberOfAllocations,
            long peakThroughput,
            long throughputLastPeriod,
            Double avgInferenceTimeLastPeriod,
            Long cacheHitCountLastPeriod
        ) {
            this.node = node;
            this.inferenceCount = inferenceCount;
            this.avgInferenceTime = avgInferenceTime;
            this.avgInferenceTimeExcludingCacheHit = avgInferenceTimeExcludingCacheHit;
            this.lastAccess = lastAccess;
            this.pendingCount = pendingCount;
            this.errorCount = errorCount;
            this.cacheHitCount = cacheHitCount;
            this.rejectedExecutionCount = rejectedExecutionCount;
            this.timeoutCount = timeoutCount;
            this.routingState = routingState;
            this.startTime = startTime;
            this.threadsPerAllocation = threadsPerAllocation;
            this.numberOfAllocations = numberOfAllocations;
            this.peakThroughput = peakThroughput;
            this.throughputLastPeriod = throughputLastPeriod;
            this.avgInferenceTimeLastPeriod = avgInferenceTimeLastPeriod;
            this.cacheHitCountLastPeriod = cacheHitCountLastPeriod;

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
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_1_0)) {
                this.threadsPerAllocation = in.readOptionalVInt();
                this.numberOfAllocations = in.readOptionalVInt();
                this.errorCount = in.readVInt();
                this.rejectedExecutionCount = in.readVInt();
                this.timeoutCount = in.readVInt();
            } else {
                this.threadsPerAllocation = null;
                this.numberOfAllocations = null;
                this.errorCount = 0;
                this.rejectedExecutionCount = 0;
                this.timeoutCount = 0;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
                this.peakThroughput = in.readVLong();
                this.throughputLastPeriod = in.readVLong();
                this.avgInferenceTimeLastPeriod = in.readOptionalDouble();
            } else {
                this.peakThroughput = 0;
                this.throughputLastPeriod = 0;
                this.avgInferenceTimeLastPeriod = null;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                this.cacheHitCount = in.readOptionalVLong();
                this.cacheHitCountLastPeriod = in.readOptionalVLong();
            } else {
                this.cacheHitCount = null;
                this.cacheHitCountLastPeriod = null;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_5_0)) {
                this.avgInferenceTimeExcludingCacheHit = in.readOptionalDouble();
            } else {
                this.avgInferenceTimeExcludingCacheHit = null;
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

        public Optional<Double> getAvgInferenceTimeExcludingCacheHit() {
            return Optional.ofNullable(avgInferenceTimeExcludingCacheHit);
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

        public Optional<Long> getCacheHitCount() {
            return Optional.ofNullable(cacheHitCount);
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

        public Integer getThreadsPerAllocation() {
            return threadsPerAllocation;
        }

        public Integer getNumberOfAllocations() {
            return numberOfAllocations;
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

        public Optional<Long> getCacheHitCountLastPeriod() {
            return Optional.ofNullable(cacheHitCountLastPeriod);
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
            if (inferenceCount != null && inferenceCount > 0) {
                if (avgInferenceTime != null) {
                    builder.field("average_inference_time_ms", avgInferenceTime);
                }
                if (avgInferenceTimeExcludingCacheHit != null) {
                    builder.field("average_inference_time_ms_excluding_cache_hits", avgInferenceTimeExcludingCacheHit);
                }
            }
            if (cacheHitCount != null) {
                builder.field("inference_cache_hit_count", cacheHitCount);
            }
            if (lastAccess != null) {
                builder.timestampFieldsFromUnixEpochMillis("last_access", "last_access_string", lastAccess.toEpochMilli());
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
                builder.timestampFieldsFromUnixEpochMillis("start_time", "start_time_string", startTime.toEpochMilli());
            }
            if (threadsPerAllocation != null) {
                builder.field("threads_per_allocation", threadsPerAllocation);
            }
            if (numberOfAllocations != null) {
                builder.field("number_of_allocations", numberOfAllocations);
            }
            builder.field("peak_throughput_per_minute", peakThroughput);
            builder.field("throughput_last_minute", throughputLastPeriod);
            if (avgInferenceTimeLastPeriod != null) {
                builder.field("average_inference_time_ms_last_minute", avgInferenceTimeLastPeriod);
            }
            if (cacheHitCountLastPeriod != null) {
                builder.field("inference_cache_hit_count_last_minute", cacheHitCountLastPeriod);
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
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_1_0)) {
                out.writeOptionalVInt(threadsPerAllocation);
                out.writeOptionalVInt(numberOfAllocations);
                out.writeVInt(errorCount);
                out.writeVInt(rejectedExecutionCount);
                out.writeVInt(timeoutCount);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
                out.writeVLong(peakThroughput);
                out.writeVLong(throughputLastPeriod);
                out.writeOptionalDouble(avgInferenceTimeLastPeriod);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                out.writeOptionalVLong(cacheHitCount);
                out.writeOptionalVLong(cacheHitCountLastPeriod);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_5_0)) {
                out.writeOptionalDouble(avgInferenceTimeExcludingCacheHit);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AssignmentStats.NodeStats that = (AssignmentStats.NodeStats) o;
            return Objects.equals(inferenceCount, that.inferenceCount)
                && Objects.equals(that.avgInferenceTime, avgInferenceTime)
                && Objects.equals(that.avgInferenceTimeExcludingCacheHit, avgInferenceTimeExcludingCacheHit)
                && Objects.equals(node, that.node)
                && Objects.equals(lastAccess, that.lastAccess)
                && Objects.equals(pendingCount, that.pendingCount)
                && Objects.equals(errorCount, that.errorCount)
                && Objects.equals(cacheHitCount, that.cacheHitCount)
                && Objects.equals(rejectedExecutionCount, that.rejectedExecutionCount)
                && Objects.equals(timeoutCount, that.timeoutCount)
                && Objects.equals(routingState, that.routingState)
                && Objects.equals(startTime, that.startTime)
                && Objects.equals(threadsPerAllocation, that.threadsPerAllocation)
                && Objects.equals(numberOfAllocations, that.numberOfAllocations)
                && Objects.equals(peakThroughput, that.peakThroughput)
                && Objects.equals(throughputLastPeriod, that.throughputLastPeriod)
                && Objects.equals(avgInferenceTimeLastPeriod, that.avgInferenceTimeLastPeriod)
                && Objects.equals(cacheHitCountLastPeriod, that.cacheHitCountLastPeriod);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                node,
                inferenceCount,
                avgInferenceTime,
                avgInferenceTimeExcludingCacheHit,
                lastAccess,
                pendingCount,
                errorCount,
                cacheHitCount,
                rejectedExecutionCount,
                timeoutCount,
                routingState,
                startTime,
                threadsPerAllocation,
                numberOfAllocations,
                peakThroughput,
                throughputLastPeriod,
                avgInferenceTimeLastPeriod,
                cacheHitCountLastPeriod
            );
        }
    }

    private final String deploymentId;
    private final String modelId;
    private AssignmentState state;
    private AllocationStatus allocationStatus;
    private String reason;
    @Nullable
    private final Integer threadsPerAllocation;
    @Nullable
    private final Integer numberOfAllocations;
    @Nullable
    private final AdaptiveAllocationsSettings adaptiveAllocationsSettings;
    @Nullable
    private final Integer queueCapacity;
    @Nullable
    private final ByteSizeValue cacheSize;
    private final Priority priority;
    private final Instant startTime;
    private final List<AssignmentStats.NodeStats> nodeStats;

    public AssignmentStats(AssignmentStats other) {
        this.deploymentId = other.deploymentId;
        this.modelId = other.modelId;
        this.threadsPerAllocation = other.threadsPerAllocation;
        this.numberOfAllocations = other.numberOfAllocations;
        this.adaptiveAllocationsSettings = other.adaptiveAllocationsSettings;
        this.queueCapacity = other.queueCapacity;
        this.startTime = other.startTime;
        this.nodeStats = other.nodeStats;
        this.state = other.state;
        this.reason = other.reason;
        this.allocationStatus = other.allocationStatus;
        this.cacheSize = other.cacheSize;
        this.priority = other.priority;
    }

    public AssignmentStats(
        String deploymentId,
        String modelId,
        @Nullable Integer threadsPerAllocation,
        @Nullable Integer numberOfAllocations,
        @Nullable AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        @Nullable Integer queueCapacity,
        @Nullable ByteSizeValue cacheSize,
        Instant startTime,
        List<AssignmentStats.NodeStats> nodeStats,
        Priority priority
    ) {
        this.deploymentId = deploymentId;
        this.modelId = modelId;
        this.threadsPerAllocation = threadsPerAllocation;
        this.numberOfAllocations = numberOfAllocations;
        this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
        this.queueCapacity = queueCapacity;
        this.startTime = Objects.requireNonNull(startTime);
        this.nodeStats = nodeStats;
        this.cacheSize = cacheSize;
        this.state = null;
        this.reason = null;
        this.priority = Objects.requireNonNull(priority);
    }

    public AssignmentStats(StreamInput in) throws IOException {
        modelId = in.readString();
        threadsPerAllocation = in.readOptionalVInt();
        numberOfAllocations = in.readOptionalVInt();
        queueCapacity = in.readOptionalVInt();
        startTime = in.readInstant();
        nodeStats = in.readCollectionAsList(AssignmentStats.NodeStats::new);
        state = in.readOptionalEnum(AssignmentState.class);
        reason = in.readOptionalString();
        allocationStatus = in.readOptionalWriteable(AllocationStatus::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            cacheSize = in.readOptionalWriteable(ByteSizeValue::readFrom);
        } else {
            cacheSize = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
            priority = in.readEnum(Priority.class);
        } else {
            priority = Priority.NORMAL;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            deploymentId = in.readString();
        } else {
            deploymentId = modelId;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            adaptiveAllocationsSettings = in.readOptionalWriteable(AdaptiveAllocationsSettings::new);
        } else {
            adaptiveAllocationsSettings = null;
        }
    }

    public String getDeploymentId() {
        return deploymentId;
    }

    public String getModelId() {
        return modelId;
    }

    @Nullable
    public Integer getThreadsPerAllocation() {
        return threadsPerAllocation;
    }

    @Nullable
    public Integer getNumberOfAllocations() {
        return numberOfAllocations;
    }

    @Nullable
    public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
        return adaptiveAllocationsSettings;
    }

    @Nullable
    public Integer getQueueCapacity() {
        return queueCapacity;
    }

    @Nullable
    public ByteSizeValue getCacheSize() {
        return cacheSize;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public List<AssignmentStats.NodeStats> getNodeStats() {
        return nodeStats;
    }

    public AssignmentState getState() {
        return state;
    }

    public AssignmentStats setNodeStats(List<AssignmentStats.NodeStats> nodeStats) {
        this.nodeStats.clear();
        this.nodeStats.addAll(nodeStats);
        return this;
    }

    public AssignmentStats setState(AssignmentState state) {
        this.state = state;
        return this;
    }

    public AssignmentStats setAllocationStatus(AllocationStatus allocationStatus) {
        this.allocationStatus = allocationStatus;
        return this;
    }

    public String getReason() {
        return reason;
    }

    public AssignmentStats setReason(String reason) {
        this.reason = reason;
        return this;
    }

    public Priority getPriority() {
        return priority;
    }

    /**
     * @return The overall inference stats for the assignment
     */
    public InferenceStats getOverallInferenceStats() {
        return new InferenceStats(
            0L,
            nodeStats.stream().filter(n -> n.getInferenceCount().isPresent()).mapToLong(n -> n.getInferenceCount().get()).sum(),
            // This is for ALL failures, so sum the error counts, timeouts, and rejections
            nodeStats.stream().mapToLong(n -> n.getErrorCount() + n.getTimeoutCount() + n.getRejectedExecutionCount()).sum(),
            // The number below is a cache miss count for the JVM model cache. We know the cache hit count for
            // the inference cache in the native process, but that's completely different, so it doesn't make
            // sense to reuse the same field here.
            // TODO: consider adding another field here for inference cache hits, but mindful of the naming collision
            0L,
            modelId,
            null,
            Instant.now()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("deployment_id", deploymentId);
        builder.field("model_id", modelId);
        if (threadsPerAllocation != null) {
            builder.field(StartTrainedModelDeploymentAction.TaskParams.THREADS_PER_ALLOCATION.getPreferredName(), threadsPerAllocation);
        }
        if (numberOfAllocations != null) {
            builder.field(StartTrainedModelDeploymentAction.TaskParams.NUMBER_OF_ALLOCATIONS.getPreferredName(), numberOfAllocations);
        }
        if (adaptiveAllocationsSettings != null) {
            builder.field(StartTrainedModelDeploymentAction.Request.ADAPTIVE_ALLOCATIONS.getPreferredName(), adaptiveAllocationsSettings);
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
        if (cacheSize != null) {
            builder.field("cache_size", cacheSize);
        }
        builder.field("priority", priority);
        builder.timestampFieldsFromUnixEpochMillis("start_time", "start_time_string", startTime.toEpochMilli());

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
        for (AssignmentStats.NodeStats nodeStat : nodeStats) {
            nodeStat.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalVInt(threadsPerAllocation);
        out.writeOptionalVInt(numberOfAllocations);
        out.writeOptionalVInt(queueCapacity);
        out.writeInstant(startTime);
        out.writeCollection(nodeStats);
        if (AssignmentState.FAILED.equals(state) && out.getTransportVersion().before(TransportVersions.V_8_4_0)) {
            out.writeOptionalEnum(AssignmentState.STARTING);
        } else {
            out.writeOptionalEnum(state);
        }
        out.writeOptionalString(reason);
        out.writeOptionalWriteable(allocationStatus);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            out.writeOptionalWriteable(cacheSize);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
            out.writeEnum(priority);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeString(deploymentId);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeOptionalWriteable(adaptiveAllocationsSettings);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AssignmentStats that = (AssignmentStats) o;
        return Objects.equals(deploymentId, that.deploymentId)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(threadsPerAllocation, that.threadsPerAllocation)
            && Objects.equals(numberOfAllocations, that.numberOfAllocations)
            && Objects.equals(adaptiveAllocationsSettings, that.adaptiveAllocationsSettings)
            && Objects.equals(queueCapacity, that.queueCapacity)
            && Objects.equals(startTime, that.startTime)
            && Objects.equals(state, that.state)
            && Objects.equals(reason, that.reason)
            && Objects.equals(allocationStatus, that.allocationStatus)
            && Objects.equals(cacheSize, that.cacheSize)
            && Objects.equals(nodeStats, that.nodeStats)
            && priority == that.priority;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            deploymentId,
            modelId,
            threadsPerAllocation,
            numberOfAllocations,
            adaptiveAllocationsSettings,
            queueCapacity,
            startTime,
            nodeStats,
            state,
            reason,
            allocationStatus,
            cacheSize,
            priority
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
