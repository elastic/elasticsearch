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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class CreateTrainedModelAssignmentAction extends ActionType<CreateTrainedModelAssignmentAction.Response> {
    public static final CreateTrainedModelAssignmentAction INSTANCE = new CreateTrainedModelAssignmentAction();
    public static final String NAME = "cluster:internal/xpack/ml/model_allocation/create";

    private CreateTrainedModelAssignmentAction() {
        super(NAME, CreateTrainedModelAssignmentAction.Response::new);
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

        private static final ParseField ASSIGNMENT = new ParseField("assignment");

        private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            "create_trained_model_assignment_response",
            a -> new Response((TrainedModelAssignment) a[0])
        );
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> TrainedModelAssignment.fromXContent(p), ASSIGNMENT);
        }

        static Response fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final TrainedModelAssignment trainedModelAssignment;

        public Response(TrainedModelAssignment trainedModelAssignment) {
            this.trainedModelAssignment = trainedModelAssignment;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.trainedModelAssignment = new TrainedModelAssignment(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            trainedModelAssignment.writeTo(out);
        }

        public TrainedModelAssignment getTrainedModelAssignment() {
            return trainedModelAssignment;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ASSIGNMENT.getPreferredName(), trainedModelAssignment);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(trainedModelAssignment, response.trainedModelAssignment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(trainedModelAssignment);
        }
    }

}
