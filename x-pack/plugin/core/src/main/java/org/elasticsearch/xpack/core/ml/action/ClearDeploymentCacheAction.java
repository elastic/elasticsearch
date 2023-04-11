/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class ClearDeploymentCacheAction extends ActionType<ClearDeploymentCacheAction.Response> {
    public static final ClearDeploymentCacheAction INSTANCE = new ClearDeploymentCacheAction();
    public static final String NAME = "cluster:admin/xpack/ml/trained_models/deployment/clear_cache";

    private ClearDeploymentCacheAction() {
        super(NAME, Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> {
        private final String deploymentId;

        public Request(String deploymentId) {
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, InferModelAction.Request.ID);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.deploymentId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(deploymentId);
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        @Override
        public boolean match(Task task) {
            return StartTrainedModelDeploymentAction.TaskMatcher.match(task, deploymentId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(deploymentId, request.deploymentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(deploymentId);
        }
    }

    public static class Response extends BaseTasksResponse implements ToXContentObject {

        private final boolean cleared;

        public Response(boolean cleared) {
            super(Collections.emptyList(), Collections.emptyList());
            this.cleared = cleared;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.cleared = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(cleared);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("cleared", cleared);
            builder.endObject();
            return builder;
        }
    }
}
