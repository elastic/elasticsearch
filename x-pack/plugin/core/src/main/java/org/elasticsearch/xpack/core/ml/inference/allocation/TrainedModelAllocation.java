/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

// TODO implement better diffable logic so that whole diff does not need to be serialized if only one part changes
/**
 * Trained model allocation object that contains allocation options and the allocation routing table
 */
public class TrainedModelAllocation implements SimpleDiffable<TrainedModelAllocation>, ToXContentObject {

    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField ALLOCATION_STATE = new ParseField("allocation_state");
    private static final ParseField ROUTING_TABLE = new ParseField("routing_table");
    private static final ParseField TASK_PARAMETERS = new ParseField("task_parameters");
    private static final ParseField START_TIME = new ParseField("start_time");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TrainedModelAllocation, Void> PARSER = new ConstructingObjectParser<>(
        "trained_model_allocation",
        true,
        a -> new TrainedModelAllocation(
            (StartTrainedModelDeploymentAction.TaskParams) a[0],
            (Map<String, RoutingStateAndReason>) a[1],
            AllocationState.fromString((String) a[2]),
            (String) a[3],
            (Instant) a[4]
        )
    );
    static {
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> StartTrainedModelDeploymentAction.TaskParams.fromXContent(p),
            TASK_PARAMETERS
        );
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(LinkedHashMap::new, RoutingStateAndReason::fromXContent),
            ROUTING_TABLE
        );
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ALLOCATION_STATE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, START_TIME.getPreferredName()),
            START_TIME,
            ObjectParser.ValueType.VALUE
        );
    }

    private final StartTrainedModelDeploymentAction.TaskParams taskParams;
    private final Map<String, RoutingStateAndReason> nodeRoutingTable;
    private final AllocationState allocationState;
    private final String reason;
    private final Instant startTime;

    public static TrainedModelAllocation fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    TrainedModelAllocation(
        StartTrainedModelDeploymentAction.TaskParams taskParams,
        Map<String, RoutingStateAndReason> nodeRoutingTable,
        AllocationState allocationState,
        String reason,
        Instant startTime
    ) {
        this.taskParams = ExceptionsHelper.requireNonNull(taskParams, TASK_PARAMETERS);
        this.nodeRoutingTable = ExceptionsHelper.requireNonNull(nodeRoutingTable, ROUTING_TABLE);
        this.allocationState = ExceptionsHelper.requireNonNull(allocationState, ALLOCATION_STATE);
        this.reason = reason;
        this.startTime = ExceptionsHelper.requireNonNull(startTime, START_TIME);
    }

    public TrainedModelAllocation(StreamInput in) throws IOException {
        this.taskParams = new StartTrainedModelDeploymentAction.TaskParams(in);
        this.nodeRoutingTable = in.readOrderedMap(StreamInput::readString, RoutingStateAndReason::new);
        this.allocationState = in.readEnum(AllocationState.class);
        this.reason = in.readOptionalString();
        this.startTime = in.readInstant();
    }

    public boolean isRoutedToNode(String nodeId) {
        return nodeRoutingTable.containsKey(nodeId);
    }

    public Map<String, RoutingStateAndReason> getNodeRoutingTable() {
        return Collections.unmodifiableMap(nodeRoutingTable);
    }

    public String getModelId() {
        return taskParams.getModelId();
    }

    public StartTrainedModelDeploymentAction.TaskParams getTaskParams() {
        return taskParams;
    }

    public AllocationState getAllocationState() {
        return allocationState;
    }

    public String[] getStartedNodes() {
        return nodeRoutingTable.entrySet()
            .stream()
            .filter(entry -> RoutingState.STARTED.equals(entry.getValue().getState()))
            .map(Map.Entry::getKey)
            .toArray(String[]::new);
    }

    public Optional<String> getReason() {
        return Optional.ofNullable(reason);
    }

    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelAllocation that = (TrainedModelAllocation) o;
        return Objects.equals(nodeRoutingTable, that.nodeRoutingTable)
            && Objects.equals(taskParams, that.taskParams)
            && Objects.equals(reason, that.reason)
            && Objects.equals(allocationState, that.allocationState)
            && Objects.equals(startTime, that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeRoutingTable, taskParams, allocationState, reason, startTime);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_PARAMETERS.getPreferredName(), taskParams);
        builder.field(ROUTING_TABLE.getPreferredName(), nodeRoutingTable);
        builder.field(ALLOCATION_STATE.getPreferredName(), allocationState);
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        builder.timeField(START_TIME.getPreferredName(), startTime);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskParams.writeTo(out);
        out.writeMap(nodeRoutingTable, StreamOutput::writeString, (o, w) -> w.writeTo(o));
        out.writeEnum(allocationState);
        out.writeOptionalString(reason);
        out.writeInstant(startTime);
    }

    public Optional<AllocationStatus> calculateAllocationStatus(List<DiscoveryNode> allocatableNodes) {
        if (allocationState.equals(AllocationState.STOPPING)) {
            return Optional.empty();
        }
        int numAllocatableNodes = 0;
        int numStarted = 0;
        for (DiscoveryNode node : allocatableNodes) {
            if (StartTrainedModelDeploymentAction.TaskParams.mayAllocateToNode(node)) {
                RoutingState nodeState = Optional.ofNullable(nodeRoutingTable.get(node.getId()))
                    .map(RoutingStateAndReason::getState)
                    .orElse(RoutingState.STOPPED);
                numAllocatableNodes++;
                if (nodeState.equals(RoutingState.STARTED)) {
                    numStarted++;
                }
            }
        }
        return Optional.of(new AllocationStatus(numStarted, numAllocatableNodes));
    }

    public static class Builder {
        private final Map<String, RoutingStateAndReason> nodeRoutingTable;
        private final StartTrainedModelDeploymentAction.TaskParams taskParams;
        private AllocationState allocationState;
        private boolean isChanged;
        private String reason;
        private Instant startTime;

        public static Builder fromAllocation(TrainedModelAllocation allocation) {
            return new Builder(
                allocation.taskParams,
                allocation.nodeRoutingTable,
                allocation.allocationState,
                allocation.reason,
                allocation.startTime
            );
        }

        public static Builder empty(StartTrainedModelDeploymentAction.TaskParams taskParams) {
            return new Builder(taskParams);
        }

        private Builder(
            StartTrainedModelDeploymentAction.TaskParams taskParams,
            Map<String, RoutingStateAndReason> nodeRoutingTable,
            AllocationState allocationState,
            String reason,
            Instant startTime
        ) {
            this.taskParams = taskParams;
            this.nodeRoutingTable = new LinkedHashMap<>(nodeRoutingTable);
            this.allocationState = allocationState;
            this.reason = reason;
            this.startTime = startTime;
        }

        private Builder(StartTrainedModelDeploymentAction.TaskParams taskParams) {
            this(taskParams, new LinkedHashMap<>(), AllocationState.STARTING, null, Instant.now());
        }

        public Builder addNewRoutingEntry(String nodeId) {
            if (nodeRoutingTable.containsKey(nodeId)) {
                throw new ResourceAlreadyExistsException(
                    "routing entry for node [{}] for model [{}] already exists",
                    nodeId,
                    taskParams.getModelId()
                );
            }
            isChanged = true;
            nodeRoutingTable.put(nodeId, new RoutingStateAndReason(RoutingState.STARTING, ""));
            return this;
        }

        // For testing purposes
        Builder addRoutingEntry(String nodeId, RoutingState state) {
            nodeRoutingTable.put(nodeId, new RoutingStateAndReason(state, ""));
            return this;
        }

        public Builder addNewFailedRoutingEntry(String nodeId, String failureReason) {
            if (nodeRoutingTable.containsKey(nodeId)) {
                throw new ResourceAlreadyExistsException(
                    "routing entry for node [{}] for model [{}] already exists",
                    nodeId,
                    taskParams.getModelId()
                );
            }
            isChanged = true;
            nodeRoutingTable.put(nodeId, new RoutingStateAndReason(RoutingState.FAILED, failureReason));
            return this;
        }

        public Builder updateExistingRoutingEntry(String nodeId, RoutingStateAndReason state) {
            RoutingStateAndReason stateAndReason = nodeRoutingTable.get(nodeId);
            if (stateAndReason == null) {
                throw new ResourceNotFoundException(
                    "routing entry for node [{}] for model [{}] does not exist",
                    nodeId,
                    taskParams.getModelId()
                );
            }
            if (stateAndReason.equals(state)) {
                return this;
            }
            nodeRoutingTable.put(nodeId, state);
            isChanged = true;
            return this;
        }

        public Builder removeRoutingEntry(String nodeId) {
            if (nodeRoutingTable.remove(nodeId) != null) {
                isChanged = true;
            }
            return this;
        }

        public Builder setReason(String reason) {
            if (Objects.equals(reason, this.reason)) {
                return this;
            }
            isChanged = true;
            this.reason = reason;
            return this;
        }

        public Builder stopAllocation(String stopReason) {
            if (allocationState.equals(AllocationState.STOPPING)) {
                return this;
            }
            isChanged = true;
            this.reason = stopReason;
            allocationState = AllocationState.STOPPING;
            return this;
        }

        public AllocationState calculateAllocationState() {
            if (allocationState.equals(AllocationState.STOPPING)) {
                return allocationState;
            }
            if (nodeRoutingTable.values().stream().anyMatch(r -> r.getState().equals(RoutingState.STARTED))) {
                return AllocationState.STARTED;
            }
            return AllocationState.STARTING;
        }

        public Builder calculateAndSetAllocationState() {
            return setAllocationState(calculateAllocationState());
        }

        public Builder setAllocationState(AllocationState state) {
            if (allocationState.equals(AllocationState.STOPPING)) {
                return this;
            }
            if (allocationState.equals(state)) {
                return this;
            }
            isChanged = true;
            allocationState = state;
            return this;
        }

        public Builder clearReason() {
            if (this.reason == null) {
                return this;
            }
            isChanged = true;
            reason = null;
            return this;
        }

        public boolean isChanged() {
            return isChanged;
        }

        public TrainedModelAllocation build() {
            return new TrainedModelAllocation(taskParams, nodeRoutingTable, allocationState, reason, startTime);
        }
    }

}
