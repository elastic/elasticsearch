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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationStats;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetDeploymentStatsAction extends ActionType<GetDeploymentStatsAction.Response> {

    public static final GetDeploymentStatsAction INSTANCE = new GetDeploymentStatsAction();
    public static final String NAME = "cluster:internal/xpack/ml/trained_models/deployments/stats/get";

    private GetDeploymentStatsAction() {
        super(NAME, GetDeploymentStatsAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<GetDeploymentStatsAction.Request> {

        private final String deploymentId;
        // used internally this should not be set by the REST request
        private List<String> expandedIds;

        public Request(String deploymentId) {
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, "deployment_id");
            this.expandedIds = Collections.singletonList(deploymentId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.deploymentId = in.readString();
            this.expandedIds = in.readStringList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(deploymentId);
            out.writeStringCollection(expandedIds);
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public void setExpandedIds(List<String> expandedIds) {
            this.expandedIds = expandedIds;
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
            return Objects.equals(deploymentId, request.deploymentId) && Objects.equals(expandedIds, request.expandedIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(deploymentId, expandedIds);
        }
    }

    public static class Response extends BaseTasksResponse implements ToXContentObject {

        public static final ParseField DEPLOYMENT_STATS = new ParseField("deployment_stats");

        private final QueryPage<AllocationStats> stats;

        public Response(
            List<TaskOperationFailure> taskFailures,
            List<? extends ElasticsearchException> nodeFailures,
            List<AllocationStats> stats,
            long count
        ) {
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

    }
}
