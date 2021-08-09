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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

        public List<String> getExpandedIds() {
            return expandedIds;
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
                private final long inferenceCount;
                private final double avgInferenceTime;
                private final Instant lastAccess;

                public NodeStats(DiscoveryNode node,
                                 long inferenceCount,
                                 double avgInferenceTime,
                                 Instant lastAccess) {
                    this.node = node;
                    this.inferenceCount = inferenceCount;
                    this.avgInferenceTime = avgInferenceTime;
                    this.lastAccess = lastAccess;
                    // if lastAccess time is null there have been no inferences
                    assert this.lastAccess != null || inferenceCount == 0;
                }

                public NodeStats(StreamInput in) throws IOException {
                    this.node = in.readOptionalWriteable(DiscoveryNode::new);
                    this.inferenceCount = in.readLong();
                    this.avgInferenceTime = in.readDouble();
                    this.lastAccess = in.readOptionalInstant();
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.startObject();
                    builder.startObject("node");
                    node.toXContent(builder, params);
                    builder.endObject();
                    builder.field("inference_count", inferenceCount);
                    builder.field("average_inference_time_ms", avgInferenceTime);
                    if (lastAccess != null) {
                        builder.timeField("last_access", "last_access_string", lastAccess.toEpochMilli());
                    }
                    builder.endObject();
                    return builder;
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeOptionalWriteable(node);
                    out.writeLong(inferenceCount);
                    out.writeDouble(avgInferenceTime);
                    out.writeOptionalInstant(lastAccess);
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    NodeStats that = (NodeStats) o;
                    return inferenceCount == that.inferenceCount &&
                        Double.compare(that.avgInferenceTime, avgInferenceTime) == 0 &&
                        Objects.equals(node, that.node) &&
                        Objects.equals(lastAccess, that.lastAccess);
                }

                @Override
                public int hashCode() {
                    return Objects.hash(node, inferenceCount, avgInferenceTime, lastAccess);
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

            public List<NodeStats> getNodeStats() {
                return nodeStats;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("model_id", modelId);
                builder.field("model_size", modelSize);
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
    }
}
