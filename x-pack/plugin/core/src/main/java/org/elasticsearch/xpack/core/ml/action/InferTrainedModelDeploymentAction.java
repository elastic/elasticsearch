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
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class InferTrainedModelDeploymentAction extends ActionType<InferTrainedModelDeploymentAction.Response> {

    public static final InferTrainedModelDeploymentAction INSTANCE = new InferTrainedModelDeploymentAction();

    // TODO Review security level
    public static final String NAME = "cluster:monitor/xpack/ml/trained_models/deployment/infer";

    public InferTrainedModelDeploymentAction() {
        super(NAME, InferTrainedModelDeploymentAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        public static final String DEPLOYMENT_ID = "deployment_id";
        public static final ParseField INPUT = new ParseField("input");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);
        static {
            PARSER.declareString((request, inputs) -> request.input = inputs, INPUT);
        }

        public static Request parseRequest(String deploymentId, XContentParser parser) {
            Request r = PARSER.apply(parser, null);
            r.deploymentId = deploymentId;
            return r;
        }

        private String deploymentId;
        private String input;

        private Request() {
        }

        public Request(String deploymentId, String input) {
            this.deploymentId = Objects.requireNonNull(deploymentId);
            this.input = Objects.requireNonNull(input);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            deploymentId = in.readString();
            input = in.readString();
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public String getInput() {
            return input;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(deploymentId);
            out.writeString(input);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(DEPLOYMENT_ID, deploymentId);
            builder.field(INPUT.getPreferredName(), input);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean match(Task task) {
            return StartTrainedModelDeploymentAction.TaskMatcher.match(task, deploymentId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferTrainedModelDeploymentAction.Request that = (InferTrainedModelDeploymentAction.Request) o;
            return Objects.equals(deploymentId, that.deploymentId)
                && Objects.equals(input, that.input);
        }

        @Override
        public int hashCode() {
            return Objects.hash(deploymentId, input);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final InferenceResults results;

        public Response(InferenceResults result) {
            super(Collections.emptyList(), Collections.emptyList());
            this.results = Objects.requireNonNull(result);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            results = in.readNamedWriteable(InferenceResults.class);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            results.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeNamedWriteable(results);
        }
    }
}
