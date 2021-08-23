/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
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
public class TrainedModelAllocation extends AbstractDiffable<TrainedModelAllocation>
    implements
        Diffable<TrainedModelAllocation>,
        ToXContentObject {

    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField ALLOCATION_STATE = new ParseField("allocation_state");
    private static final ParseField ROUTING_TABLE = new ParseField("routing_table");
    private static final ParseField TASK_PARAMETERS = new ParseField("task_parameters");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TrainedModelAllocation, Void> PARSER = new ConstructingObjectParser<>(
        "trained_model_allocation",
        true,
        a -> new TrainedModelAllocation(
            (StartTrainedModelDeploymentAction.TaskParams) a[0],
            (Map<String, RoutingStateAndReason>) a[1],
            AllocationState.fromString((String)a[2]),
            (String) a[3]
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
    }

    private final StartTrainedModelDeploymentAction.TaskParams taskParams;
    private final Map<String, RoutingStateAndReason> nodeRoutingTable;
    private final AllocationState allocationState;
    private final String reason;

    public static TrainedModelAllocation fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    TrainedModelAllocation(
        StartTrainedModelDeploymentAction.TaskParams taskParams,
        Map<String, RoutingStateAndReason> nodeRoutingTable,
        AllocationState allocationState,
        String reason
    ) {
        this.taskParams = ExceptionsHelper.requireNonNull(taskParams, TASK_PARAMETERS);
        this.nodeRoutingTable = ExceptionsHelper.requireNonNull(nodeRoutingTable, ROUTING_TABLE);
        this.allocationState = ExceptionsHelper.requireNonNull(allocationState, ALLOCATION_STATE);
        this.reason = reason;
    }

    public TrainedModelAllocation(StreamInput in) throws IOException {
        this.taskParams = new StartTrainedModelDeploymentAction.TaskParams(in);
        this.nodeRoutingTable = in.readOrderedMap(StreamInput::readString, RoutingStateAndReason::new);
        this.allocationState = in.readEnum(AllocationState.class);
        this.reason = in.readOptionalString();
    }

    public boolean isRoutedToNode(String nodeId) {
        return nodeRoutingTable.containsKey(nodeId);
    }

    public Map<String, RoutingStateAndReason> getNodeRoutingTable() {
        return Collections.unmodifiableMap(nodeRoutingTable);
    }

    public StartTrainedModelDeploymentAction.TaskParams getTaskParams() {
        return taskParams;
    }

    public AllocationState getAllocationState() {
        return allocationState;
    }

    public String[] getStartedNodes() {
        return nodeRoutingTable
            .entrySet()
            .stream()
            .filter(entry -> RoutingState.STARTED.equals(entry.getValue().getState()))
            .map(Map.Entry::getKey)
            .toArray(String[]::new);
    }

    public Optional<String> getReason() {
        return Optional.ofNullable(reason);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelAllocation that = (TrainedModelAllocation) o;
        return Objects.equals(nodeRoutingTable, that.nodeRoutingTable)
            && Objects.equals(taskParams, that.taskParams)
            && Objects.equals(reason, that.reason)
            && Objects.equals(allocationState, that.allocationState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeRoutingTable, taskParams, allocationState, reason);
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
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskParams.writeTo(out);
        out.writeMap(nodeRoutingTable, StreamOutput::writeString, (o, w) -> w.writeTo(o));
        out.writeEnum(allocationState);
        out.writeOptionalString(reason);
    }

    public static class Builder {
        private final Map<String, RoutingStateAndReason> nodeRoutingTable;
        private final StartTrainedModelDeploymentAction.TaskParams taskParams;
        private AllocationState allocationState;
        private boolean isChanged;
        private String reason;

        public static Builder fromAllocation(TrainedModelAllocation allocation) {
            return new Builder(allocation.taskParams, allocation.nodeRoutingTable, allocation.allocationState, allocation.reason);
        }

        public static Builder empty(StartTrainedModelDeploymentAction.TaskParams taskParams) {
            return new Builder(taskParams);
        }

        private Builder(
            StartTrainedModelDeploymentAction.TaskParams taskParams,
            Map<String, RoutingStateAndReason> nodeRoutingTable,
            AllocationState allocationState,
            String reason
        ) {
            this.taskParams = taskParams;
            this.nodeRoutingTable = new LinkedHashMap<>(nodeRoutingTable);
            this.allocationState = allocationState;
            this.reason = reason;
        }

        private Builder(StartTrainedModelDeploymentAction.TaskParams taskParams) {
            this.nodeRoutingTable = new LinkedHashMap<>();
            this.taskParams = taskParams;
            this.allocationState = AllocationState.STARTING;
        }

        public Builder addNewRoutingEntry(String nodeId) {
            if (nodeRoutingTable.containsKey(nodeId)) {
                throw new ResourceAlreadyExistsException(
                    "routing entry for node [{}] for model [{}] already exists", nodeId, taskParams.getModelId()
                );
            }
            isChanged = true;
            nodeRoutingTable.put(nodeId, new RoutingStateAndReason(RoutingState.STARTING, ""));
            return this;
        }

        public Builder addNewFailedRoutingEntry(String nodeId, String reason) {
            if (nodeRoutingTable.containsKey(nodeId)) {
                throw new ResourceAlreadyExistsException(
                    "routing entry for node [{}] for model [{}] already exists", nodeId, taskParams.getModelId()
                );
            }
            isChanged = true;
            nodeRoutingTable.put(nodeId, new RoutingStateAndReason(RoutingState.FAILED, reason));
            return this;
        }

        public Builder updateExistingRoutingEntry(String nodeId, RoutingStateAndReason state) {
            RoutingStateAndReason stateAndReason = nodeRoutingTable.get(nodeId);
            if (stateAndReason == null) {
                throw new ResourceNotFoundException(
                    "routing entry for node [{}] for model [{}] does not exist", nodeId, taskParams.getModelId()
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

        public Builder stopAllocation(String reason) {
            if (allocationState.equals(AllocationState.STOPPING)) {
                return this;
            }
            isChanged = true;
            this.reason = reason;
            allocationState = AllocationState.STOPPING;
            return this;
        }

        public AllocationState calculateAllocationState(List<DiscoveryNode> allocatableNodes) {
            if (allocationState.equals(AllocationState.STOPPING)) {
                return allocationState;
            }
            if (nodeRoutingTable.isEmpty()) {
                return AllocationState.STARTING;
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
            if (numStarted == 0) {
                return AllocationState.STARTING;
            }
            // TODO: Update once we don't allocate to all possible nodes
            if (numStarted < numAllocatableNodes) {
                return AllocationState.PARTIALLY_STARTED;
            }
            return AllocationState.STARTED;
        }

        public Builder calculateAndSetAllocationState(List<DiscoveryNode> allocatableNodes) {
            return setAllocationState(calculateAllocationState(allocatableNodes));
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
            return new TrainedModelAllocation(taskParams, nodeRoutingTable, allocationState, reason);
        }
    }

}
