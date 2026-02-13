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
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats.Response;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Internal (no-REST) action to retrieve metrics for serverless autoscaling.
 */
public class GetMlAutoscalingStats extends ActionType<Response> {

    public static final GetMlAutoscalingStats INSTANCE = new GetMlAutoscalingStats();
    public static final String NAME = "cluster:monitor/xpack/ml/autoscaling/stats/get";

    public GetMlAutoscalingStats() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final TimeValue requestTimeout;

        public Request(TimeValue masterNodeTimeout, TimeValue requestTimeout) {
            super(masterNodeTimeout);
            this.requestTimeout = Objects.requireNonNull(requestTimeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.requestTimeout = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeTimeValue(this.requestTimeout);
        }

        public TimeValue requestTimeout() {
            return requestTimeout;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "get_ml_autoscaling_resources", parentTaskId, headers);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestTimeout);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            GetMlAutoscalingStats.Request other = (GetMlAutoscalingStats.Request) obj;
            return Objects.equals(requestTimeout, other.requestTimeout);
        }
    }

    public static class Response extends ActionResponse {

        private final MlAutoscalingStats autoscalingResources;

        public Response(final MlAutoscalingStats autoscalingResources) {
            this.autoscalingResources = autoscalingResources;
        }

        public Response(final StreamInput in) throws IOException {
            this.autoscalingResources = new MlAutoscalingStats(in);
        }

        public MlAutoscalingStats getAutoscalingResources() {
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
            GetMlAutoscalingStats.Response other = (GetMlAutoscalingStats.Response) obj;
            return Objects.equals(autoscalingResources, other.autoscalingResources);
        }
    }
}
