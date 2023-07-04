/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingResources.Response;
import org.elasticsearch.xpack.core.ml.autoscaling.AutoscalingResources;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GetMlAutoscalingResources extends ActionType<Response> {

    public static final GetMlAutoscalingResources INSTANCE = new GetMlAutoscalingResources();
    public static final String NAME = "cluster:internal/serverless/ml/get_ml_autoscaling_resources";

    public GetMlAutoscalingResources() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public Request(TimeValue timeout) {
            super(timeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "get_ml_autoscaling_resources", parentTaskId, headers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timeout);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            GetMlAutoscalingResources.Request other = (GetMlAutoscalingResources.Request) obj;
            return Objects.equals(timeout, other.timeout);
        }
    }

    public static class Response extends ActionResponse {

        private final AutoscalingResources autoscalingResources;

        public Response(final AutoscalingResources autoscalingResources) {
            this.autoscalingResources = autoscalingResources;
        }

        public Response(final StreamInput in) throws IOException {
            super(in);
            this.autoscalingResources = new AutoscalingResources(in);
        }

        public AutoscalingResources getAutoscalingResources() {
            return autoscalingResources;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            autoscalingResources.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(autoscalingResources);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            GetMlAutoscalingResources.Response other = (GetMlAutoscalingResources.Response) obj;
            return Objects.equals(autoscalingResources, other.autoscalingResources);
        }
    }
}
