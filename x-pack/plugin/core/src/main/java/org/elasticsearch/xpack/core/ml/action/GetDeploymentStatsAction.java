/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GetDeploymentStatsAction extends ActionType<GetDeploymentStatsAction.Response> {

    public static final GetDeploymentStatsAction INSTANCE = new GetDeploymentStatsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/trained_models/deployments/stats/get";

    private GetDeploymentStatsAction() {
        super(NAME, GetDeploymentStatsAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<GetDeploymentStatsAction.Request> {

        public static final String ALLOW_NO_MATCH = "allow_no_match";

        private final String deploymentId;
        private boolean allowNoMatch = true;
        // used internally this should not be set by the REST request
        private List<String> expandedIds;

        public Request(String deploymentId) {
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, "deployment_id");
            this.expandedIds = Collections.singletonList(deploymentId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.deploymentId = in.readString();
            this.allowNoMatch = in.readBoolean();
            this.expandedIds = in.readStringList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(deploymentId);
            out.writeBoolean(allowNoMatch);
            out.writeStringCollection(expandedIds);
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public void setExpandedIds(List<String> expandedIds) {
            this.expandedIds = expandedIds;
        }

        public void setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
        }

        public boolean isAllowNoMatch() {
            return allowNoMatch;
        }

        @Override
        public boolean match(Task task) {
            return expandedIds.stream().anyMatch(taskId -> StartTrainedModelDeploymentAction.TaskMatcher.match(task, taskId));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(deploymentId, request.deploymentId) &&
                this.allowNoMatch == request.allowNoMatch &&
                Objects.equals(expandedIds, request.expandedIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(deploymentId, allowNoMatch, expandedIds);
        }
    }


    public static class Response extends BaseTasksResponse implements ToXContentObject {

        public static final ParseField DEPLOYMENT_STATS = new ParseField("deployment_stats");

        public static class AllocationStats implements ToXContentObject, Writeable {

            public static class NodeStats implements ToXContentObject, Writeable {
                private final DiscoveryNode node;
                private final Long inferenceCount;
                private final Double avgInferenceTime;
                private final Instant lastAccess;
                private final RoutingStateAndReason routingState;

                public static NodeStats forStartedState(DiscoveryNode node,
                                                 long inferenceCount,
                                                 double avgInferenceTime,
                                                 Instant lastAccess) {
                    return new NodeStats(node, inferenceCount, avgInferenceTime, lastAccess,
                        new RoutingStateAndReason(RoutingState.STARTED, null));
                }

                public static NodeStats forNotStartedState(DiscoveryNode node,
                                                           RoutingState state,
                                                           String reason) {
                    return new NodeStats(node, null, null, null,
                        new RoutingStateAndReason(state, reason));
                }

                private NodeStats(DiscoveryNode node,
                                 Long inferenceCount,
                                 Double avgInferenceTime,
                                 Instant lastAccess,
                                 RoutingStateAndReason routingState) {
                    this.node = node;
                    this.inferenceCount = inferenceCount;
                    this.avgInferenceTime = avgInferenceTime;
                    this.lastAccess = lastAccess;
                    this.routingState = routingState;

                    // if lastAccess time is null there have been no inferences
                    assert this.lastAccess != null || (inferenceCount == null || inferenceCount == 0);
                }

                public NodeStats(StreamInput in) throws IOException {
                    this.node = in.readOptionalWriteable(DiscoveryNode::new);
                    this.inferenceCount = in.readOptionalLong();
                    this.avgInferenceTime = in.readOptionalDouble();
                    this.lastAccess = in.readOptionalInstant();
                    this.routingState = in.readOptionalWriteable(RoutingStateAndReason::new);
                }

                public DiscoveryNode getNode() {
                    return node;
                }

                public RoutingStateAndReason getRoutingState() {
                    return routingState;
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
                    builder.endObject();
                    return builder;
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeOptionalWriteable(node);
                    out.writeOptionalLong(inferenceCount);
                    out.writeOptionalDouble(avgInferenceTime);
                    out.writeOptionalInstant(lastAccess);
                    out.writeOptionalWriteable(routingState);
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    NodeStats that = (NodeStats) o;
                    return Objects.equals(inferenceCount, that.inferenceCount) &&
                        Objects.equals(that.avgInferenceTime, avgInferenceTime) &&
                        Objects.equals(node, that.node) &&
                        Objects.equals(lastAccess, that.lastAccess) &&
                        Objects.equals(routingState, that.routingState);
                }

                @Override
                public int hashCode() {
                    return Objects.hash(node, inferenceCount, avgInferenceTime, lastAccess, routingState);
                }
            }


            private final String modelId;
            private final ByteSizeValue modelSize;
            private final List<NodeStats> nodeStats;

            public AllocationStats(String modelId, ByteSizeValue modelSize, List<NodeStats> nodeStats) {
                this.modelId = modelId;
                this.modelSize = modelSize;
                this.nodeStats = nodeStats;
            }

            public AllocationStats(StreamInput in) throws IOException {
                modelId = in.readString();
                modelSize = in.readOptionalWriteable(ByteSizeValue::new);
                nodeStats = in.readList(NodeStats::new);
            }

            public String getModelId() {
                return modelId;
            }

            public ByteSizeValue getModelSize() {
                return modelSize;
            }

            public List<NodeStats> getNodeStats() {
                return nodeStats;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("model_id", modelId);
                if (modelSize != null) {
                    builder.field("model_size", modelSize);
                }
                builder.startArray("nodes");
                for (NodeStats nodeStat : nodeStats){
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
                out.writeList(nodeStats);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                AllocationStats that = (AllocationStats) o;
                return Objects.equals(modelId, that.modelId) &&
                    Objects.equals(modelSize, that.modelSize) &&
                    Objects.equals(nodeStats, that.nodeStats);
            }

            @Override
            public int hashCode() {
                return Objects.hash(modelId, modelSize, nodeStats);
            }
        }

        private final QueryPage<AllocationStats> stats;

        public Response(List<TaskOperationFailure> taskFailures, List<? extends ElasticsearchException> nodeFailures,
                        List<AllocationStats> stats, long count) {
            super(taskFailures, nodeFailures);
            this.stats = new QueryPage<>(stats, count, DEPLOYMENT_STATS);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            stats = new QueryPage<>(in, AllocationStats::new);
        }

        public QueryPage<AllocationStats> getStats() {
            return stats;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            stats.doXContentBody(builder, params);
            toXContentCommon(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            stats.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            Response response = (Response) o;
            return Objects.equals(stats, response.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), stats);
        }

        /**
         * Update the collected task responses with the non-started
         * allocation information. The result is the task responses
         * merged with the non-started model allocations.
         *
         * Where there is a merge collision for the pair {@code <model_id, node_id>}
         * the non-started allocations are used.
         *
         * @param tasksResponse All the responses from the tasks
         * @param nonStartedModelRoutes Non-started routes
         * @return The result of merging tasksResponse and the non-started routes
         */
        public static GetDeploymentStatsAction.Response addFailedRoutes(
            GetDeploymentStatsAction.Response tasksResponse,
            Map<String, Map<String, RoutingStateAndReason>> nonStartedModelRoutes,
            DiscoveryNodes nodes) {

            List<GetDeploymentStatsAction.Response.AllocationStats> updatedAllocationStats = new ArrayList<>();

            for (GetDeploymentStatsAction.Response.AllocationStats stat : tasksResponse.getStats().results()) {
                if (nonStartedModelRoutes.containsKey(stat.getModelId())) {
                    // there is merging to be done
                    Map<String, RoutingStateAndReason> nodeToRoutingStates = nonStartedModelRoutes.get(stat.getModelId());
                    List<GetDeploymentStatsAction.Response.AllocationStats.NodeStats> updatedNodeStats = new ArrayList<>();

                    Set<String> visitedNodes = new HashSet<>();
                    for (var nodeStat : stat.getNodeStats()) {
                        if (nodeToRoutingStates.containsKey(nodeStat.getNode().getId())) {
                            // conflict as there is both a task response for the model/node pair
                            // and we have a non-started routing entry.
                            // Prefer the entry from nonStartedModelRoutes as we cannot be sure
                            // of the state of the task - it may be starting, started, stopping, or stopped.
                            RoutingStateAndReason stateAndReason = nodeToRoutingStates.get(nodeStat.getNode().getId());
                            updatedNodeStats.add(
                                GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forNotStartedState(
                                    nodeStat.getNode(),
                                    stateAndReason.getState(),
                                    stateAndReason.getReason()));
                        } else {
                            updatedNodeStats.add(nodeStat);
                        }

                        visitedNodes.add(nodeStat.node.getId());
                    }

                    // add nodes from the failures that were not in the task responses
                    for (var nodeRoutingState : nodeToRoutingStates.entrySet()) {
                        if (visitedNodes.contains(nodeRoutingState.getKey()) == false) {
                            updatedNodeStats.add(
                                GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forNotStartedState(
                                    nodes.get(nodeRoutingState.getKey()),
                                    nodeRoutingState.getValue().getState(),
                                    nodeRoutingState.getValue().getReason()));
                        }
                    }

                    updatedNodeStats.sort(Comparator.comparing(n -> n.getNode().getId()));
                    updatedAllocationStats.add(
                        new GetDeploymentStatsAction.Response.AllocationStats(stat.getModelId(), stat.getModelSize(), updatedNodeStats));
                } else {
                    updatedAllocationStats.add(stat);
                }
            }

            // Merge any models in the non-started that were not in the task responses
            for (var nonStartedEntries : nonStartedModelRoutes.entrySet()) {
                String modelId = nonStartedEntries.getKey();
                if (tasksResponse.getStats().results()
                    .stream()
                    .anyMatch(e -> modelId.equals(e.getModelId())) == false) {

                    // no tasks for this model so build the allocation stats from the non-started states
                    List<GetDeploymentStatsAction.Response.AllocationStats.NodeStats> nodeStats = new ArrayList<>();

                    for (var routingEntry : nonStartedEntries.getValue().entrySet()) {
                            nodeStats.add(AllocationStats.NodeStats.forNotStartedState(
                                nodes.get(routingEntry.getKey()),
                                routingEntry.getValue().getState(),
                                routingEntry.getValue().getReason()));
                    }

                    nodeStats.sort(Comparator.comparing(n -> n.getNode().getId()));

                    updatedAllocationStats.add(new GetDeploymentStatsAction.Response.AllocationStats(modelId, null, nodeStats));
                }
            }

            updatedAllocationStats.sort(Comparator.comparing(GetDeploymentStatsAction.Response.AllocationStats::getModelId));

            return new GetDeploymentStatsAction.Response(tasksResponse.getTaskFailures(),
                tasksResponse.getNodeFailures(),
                updatedAllocationStats,
                updatedAllocationStats.size());
        }
    }
}
