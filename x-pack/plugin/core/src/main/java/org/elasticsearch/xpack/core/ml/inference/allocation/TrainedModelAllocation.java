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
import java.util.Map;
import java.util.Objects;

// TODO implement better diffable logic so that whole diff does not need to be serialized if only one part changes
/**
 * Trained model allocation object that contains allocation options and the allocation routing table
 */
public class TrainedModelAllocation extends AbstractDiffable<TrainedModelAllocation>
    implements
        Diffable<TrainedModelAllocation>,
        ToXContentObject {

    private static final ParseField ROUTING_TABLE = new ParseField("routing_table");
    private static final ParseField TASK_PARAMETERS = new ParseField("task_parameters");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TrainedModelAllocation, Void> PARSER = new ConstructingObjectParser<>(
        "trained_model_allocation",
        true,
        a -> new TrainedModelAllocation((StartTrainedModelDeploymentAction.TaskParams) a[0], (Map<String, RoutingStateAndReason>) a[1])
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
    }

    private final StartTrainedModelDeploymentAction.TaskParams taskParams;
    private final Map<String, RoutingStateAndReason> nodeRoutingTable;

    public static TrainedModelAllocation fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    TrainedModelAllocation(StartTrainedModelDeploymentAction.TaskParams taskParams, Map<String, RoutingStateAndReason> nodeRoutingTable) {
        this.taskParams = ExceptionsHelper.requireNonNull(taskParams, TASK_PARAMETERS);
        this.nodeRoutingTable = ExceptionsHelper.requireNonNull(nodeRoutingTable, ROUTING_TABLE);
    }

    public TrainedModelAllocation(StreamInput in) throws IOException {
        this.taskParams = new StartTrainedModelDeploymentAction.TaskParams(in);
        nodeRoutingTable = in.readOrderedMap(StreamInput::readString, RoutingStateAndReason::new);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelAllocation that = (TrainedModelAllocation) o;
        return Objects.equals(nodeRoutingTable, that.nodeRoutingTable) && Objects.equals(taskParams, that.taskParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeRoutingTable, taskParams);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_PARAMETERS.getPreferredName(), taskParams);
        builder.field(ROUTING_TABLE.getPreferredName(), nodeRoutingTable);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskParams.writeTo(out);
        out.writeMap(nodeRoutingTable, StreamOutput::writeString, (o, w) -> w.writeTo(o));
    }

    public static class Builder {
        private final Map<String, RoutingStateAndReason> nodeRoutingTable;
        private final StartTrainedModelDeploymentAction.TaskParams taskParams;
        private boolean isChanged;

        public static Builder fromAllocation(TrainedModelAllocation allocation) {
            return new Builder(allocation.taskParams, allocation.nodeRoutingTable);
        }

        public static Builder empty(StartTrainedModelDeploymentAction.TaskParams taskParams) {
            return new Builder(taskParams);
        }

        private Builder(StartTrainedModelDeploymentAction.TaskParams taskParams, Map<String, RoutingStateAndReason> nodeRoutingTable) {
            this.taskParams = taskParams;
            this.nodeRoutingTable = new LinkedHashMap<>(nodeRoutingTable);
        }

        private Builder(StartTrainedModelDeploymentAction.TaskParams taskParams) {
            this.nodeRoutingTable = new LinkedHashMap<>();
            this.taskParams = taskParams;
        }

        public Builder addNewRoutingEntry(String nodeId) {
            if (nodeRoutingTable.containsKey(nodeId)) {
                throw new ResourceAlreadyExistsException(
                    "routing entry for node [{}] for model [{}] already exists", nodeId, taskParams.getModelId()
                );
            }
            isChanged = true;
            nodeRoutingTable.put(nodeId, new RoutingStateAndReason(RoutingState.INITIALIZING, ""));
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

        public boolean isChanged() {
            return isChanged;
        }

        public TrainedModelAllocation build() {
            return new TrainedModelAllocation(taskParams, nodeRoutingTable);
        }
    }

}
