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

        public static class DeploymentStats implements ToXContentObject, Writeable {

            private final String modelId;
            private final String node_id;
            private final long inferenceCount;
            private final double avgInferenceTime;
            private final Instant lastAccess;
            private final ByteSizeValue modelSize;

            public DeploymentStats(String modelId,
                                   String node,
                                   long inferenceCount,
                                   double avgInferenceTime,
                                   Instant lastAccess,
                                   ByteSizeValue modelSize) {
                this.modelId = modelId;
                this.node_id = node;
                this.inferenceCount = inferenceCount;
                this.avgInferenceTime = avgInferenceTime;
                this.lastAccess = lastAccess;
                this.modelSize = modelSize;
            }

            public DeploymentStats(StreamInput in) throws IOException {
                this.modelId = in.readString();
                this.node_id = in.readString();
                this.inferenceCount = in.readLong();
                this.avgInferenceTime = in.readDouble();
                this.lastAccess = in.readInstant();
                this.modelSize = in.readOptionalWriteable(ByteSizeValue::new);
            }

            public String getModelId() {
                return modelId;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("model_id", modelId);
                builder.field("node_id", node_id);
                builder.field("inference_count", inferenceCount);
                builder.field("average_inference_time_ms", avgInferenceTime);
                builder.timeField("last_access", "last_access_string", lastAccess.toEpochMilli());
                builder.field("model_size", modelSize);
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(modelId);
                out.writeString(node_id);
                out.writeLong(inferenceCount);
                out.writeDouble(avgInferenceTime);
                out.writeInstant(lastAccess);
                out.writeOptionalWriteable(modelSize);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                DeploymentStats that = (DeploymentStats) o;
                return inferenceCount == that.inferenceCount &&
                    Double.compare(that.avgInferenceTime, avgInferenceTime) == 0 &&
                    Objects.equals(modelId, that.modelId) &&
                    Objects.equals(node_id, that.node_id) &&
                    Objects.equals(lastAccess, that.lastAccess) &&
                    Objects.equals(modelSize, that.modelSize);
            }

            @Override
            public int hashCode() {
                return Objects.hash(modelId, node_id, inferenceCount, avgInferenceTime, lastAccess, modelSize);
            }
        }


        private final QueryPage<DeploymentStats> stats;

        public Response(List<TaskOperationFailure> taskFailures, List<? extends ElasticsearchException> nodeFailures,
                        List<DeploymentStats> stats, long count) {
            super(taskFailures, nodeFailures);
            this.stats = new QueryPage<>(stats, count, DEPLOYMENT_STATS);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            stats = new QueryPage<>(in, DeploymentStats::new);
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
