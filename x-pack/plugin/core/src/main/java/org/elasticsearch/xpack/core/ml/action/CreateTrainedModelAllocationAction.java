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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class CreateTrainedModelAllocationAction extends ActionType<CreateTrainedModelAllocationAction.Response> {
    public static final CreateTrainedModelAllocationAction INSTANCE = new CreateTrainedModelAllocationAction();
    public static final String NAME = "cluster:internal/xpack/ml/model_allocation/create";

    private CreateTrainedModelAllocationAction() {
        super(NAME, CreateTrainedModelAllocationAction.Response::new);
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final StartTrainedModelDeploymentAction.TaskParams taskParams;

        public Request(StartTrainedModelDeploymentAction.TaskParams taskParams) {
            this.taskParams = ExceptionsHelper.requireNonNull(taskParams, "taskParams");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.taskParams = new StartTrainedModelDeploymentAction.TaskParams(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            taskParams.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(taskParams, request.taskParams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskParams);
        }

        public StartTrainedModelDeploymentAction.TaskParams getTaskParams() {
            return taskParams;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private static final ParseField ALLOCATION = new ParseField("allocation");

        private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            "create_trained_model_allocation_response",
            a -> new Response((TrainedModelAllocation) a[0])
        );
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> TrainedModelAllocation.fromXContent(p), ALLOCATION);
        }
        static Response fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final TrainedModelAllocation trainedModelAllocation;

        public Response(TrainedModelAllocation trainedModelAllocation) {
            this.trainedModelAllocation = trainedModelAllocation;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.trainedModelAllocation = new TrainedModelAllocation(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            trainedModelAllocation.writeTo(out);
        }

        public TrainedModelAllocation getTrainedModelAllocation() {
            return trainedModelAllocation;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("allocation", trainedModelAllocation);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(trainedModelAllocation, response.trainedModelAllocation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(trainedModelAllocation);
        }
    }

}
