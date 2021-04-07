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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class InferTrainedModelDeploymentAction extends ActionType<InferTrainedModelDeploymentAction.Response> {

    public static final InferTrainedModelDeploymentAction INSTANCE = new InferTrainedModelDeploymentAction();

    // TODO Review security level
    public static final String NAME = "cluster:monitor/xpack/ml/trained_models/deployment/infer";

    public InferTrainedModelDeploymentAction() {
        super(NAME, InferTrainedModelDeploymentAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        private static final ParseField DEPLOYMENT_ID = new ParseField("deployment_id");
        public static final ParseField INPUTS = new ParseField("inputs");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>("infer_trained_model_request", Request::new);

        static {
            PARSER.declareString((request, deploymentId) -> request.deploymentId = deploymentId, DEPLOYMENT_ID);
            PARSER.declareDoubleArray(Request::setInputs, INPUTS);
        }

        public static Request parseRequest(String deploymentId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (deploymentId != null) {
                request.deploymentId = deploymentId;
            }
            return request;
        }

        private String deploymentId;
        private double[] inputs;

        private Request() {
        }

        public Request(String deploymentId) {
            this.deploymentId = Objects.requireNonNull(deploymentId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            deploymentId = in.readString();
            inputs = in.readDoubleArray();
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public void setInputs(List<Double> inputs) {
            ExceptionsHelper.requireNonNull(inputs, INPUTS);
            this.inputs = inputs.stream().mapToDouble(d -> d).toArray();
        }

        public double[] getInputs() {
            return inputs;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(deploymentId);
            out.writeDoubleArray(inputs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(DEPLOYMENT_ID.getPreferredName(), deploymentId);
            builder.array(INPUTS.getPreferredName(), inputs);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean match(Task task) {
            return StartTrainedModelDeploymentAction.TaskMatcher.match(task, deploymentId);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final PyTorchResult result;

        public Response(PyTorchResult result) {
            super(Collections.emptyList(), Collections.emptyList());
            this.result = Objects.requireNonNull(result);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            result = new PyTorchResult(in);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            result.toXContent(builder, params);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            result.writeTo(out);
        }
    }
}
