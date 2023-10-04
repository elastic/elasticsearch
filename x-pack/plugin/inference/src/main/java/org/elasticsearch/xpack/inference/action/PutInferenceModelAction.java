/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

public class PutInferenceModelAction extends ActionType<PutInferenceModelAction.Response> {

    public static final PutInferenceModelAction INSTANCE = new PutInferenceModelAction();
    public static final String NAME = "cluster:admin/xpack/inference/put";

    public PutInferenceModelAction() {
        super(NAME, PutInferenceModelAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final TaskType taskType;
        private final String modelId;
        private final BytesReference content;
        private final XContentType contentType;

        public Request(String taskType, String modelId, BytesReference content, XContentType contentType) {
            this.taskType = TaskType.fromStringOrStatusException(taskType);
            this.modelId = modelId;
            this.content = content;
            this.contentType = contentType;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.taskType = TaskType.fromStream(in);
            this.content = in.readBytesReference();
            this.contentType = in.readEnum(XContentType.class);
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getModelId() {
            return modelId;
        }

        public BytesReference getContent() {
            return content;
        }

        public XContentType getContentType() {
            return contentType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            taskType.writeTo(out);
            out.writeBytesReference(content);
            XContentHelper.writeTo(out, contentType);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return taskType == request.taskType
                && Objects.equals(modelId, request.modelId)
                && Objects.equals(content, request.content)
                && contentType == request.contentType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskType, modelId, content, contentType);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final ModelConfigurations model;

        public Response(ModelConfigurations model) {
            this.model = model;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            model = new ModelConfigurations(in);
        }

        public ModelConfigurations getModel() {
            return model;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            model.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return model.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(model, response.model);
        }

        @Override
        public int hashCode() {
            return Objects.hash(model);
        }
    }
}
