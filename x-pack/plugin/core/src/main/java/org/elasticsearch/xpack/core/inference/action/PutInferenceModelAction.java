/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.EndpointVersions;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.ModelConfigurations.ENDPOINT_VERSION_FIELD_NAME;
import static org.elasticsearch.inference.ModelConfigurations.OLD_TASK_SETTINGS;
import static org.elasticsearch.inference.ModelConfigurations.PARAMETERS;

public class PutInferenceModelAction extends ActionType<PutInferenceModelAction.Response> {

    public static final PutInferenceModelAction INSTANCE = new PutInferenceModelAction();
    public static final String NAME = "cluster:admin/xpack/inference/put";

    public PutInferenceModelAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final TaskType taskType;
        private final String inferenceEntityId;
        private final BytesReference content;
        private BytesReference rewrittenContent;
        private final XContentType contentType;

        public Request(TaskType taskType, String inferenceEntityId, BytesReference content, XContentType contentType) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
            this.taskType = taskType;
            this.inferenceEntityId = inferenceEntityId;
            this.content = content;
            this.contentType = contentType;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.taskType = TaskType.fromStream(in);
            this.content = in.readBytesReference();
            this.contentType = in.readEnum(XContentType.class);
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public BytesReference getContent() {
            return content;
        }

        public BytesReference getRewrittenContent() {
            if (rewrittenContent == null) { // rewrittenContent is deterministic on content, so we only need to calculate it once
                Map<String, Object> newContent = XContentHelper.convertToMap(content, false, contentType).v2();
                if (newContent.containsKey(PARAMETERS) && newContent.containsKey(OLD_TASK_SETTINGS)) {
                    throw new ElasticsearchStatusException(
                        "Request cannot contain both [task_settings] and [parameters], use only [parameters]",
                        RestStatus.BAD_REQUEST
                    );
                } else if (newContent.containsKey(PARAMETERS)) {
                    newContent.put(OLD_TASK_SETTINGS, newContent.get(PARAMETERS));
                    newContent.put(ENDPOINT_VERSION_FIELD_NAME, EndpointVersions.PARAMETERS_INTRODUCED_ENDPOINT_VERSION);
                    newContent.remove(PARAMETERS);
                } else if (newContent.containsKey(OLD_TASK_SETTINGS)) {
                    newContent.put(ENDPOINT_VERSION_FIELD_NAME, EndpointVersions.FIRST_ENDPOINT_VERSION);
                } else {
                    newContent.put(ENDPOINT_VERSION_FIELD_NAME, EndpointVersions.FIRST_ENDPOINT_VERSION);
                }
                try (XContentBuilder builder = XContentFactory.contentBuilder(this.contentType)) {
                    builder.map(newContent);
                    this.rewrittenContent = BytesReference.bytes(builder);
                } catch (IOException e) {
                    throw new ElasticsearchStatusException("Failed to parse rewritten request", RestStatus.INTERNAL_SERVER_ERROR, e);
                }
            }
            return rewrittenContent;
        }

        public XContentType getContentType() {
            return contentType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
            taskType.writeTo(out);
            out.writeBytesReference(content);
            XContentHelper.writeTo(out, contentType);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = new ActionRequestValidationException();
            if (MlStrings.isValidId(this.inferenceEntityId) == false) {
                validationException.addValidationError(Messages.getMessage(Messages.INVALID_ID, "inference_id", this.inferenceEntityId));
            }

            if (validationException.validationErrors().isEmpty() == false) {
                return validationException;
            } else {
                return null;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return taskType == request.taskType
                && Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && Objects.equals(content, request.content)
                && contentType == request.contentType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskType, inferenceEntityId, content, contentType);
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
            return model.toFilteredXContent(builder, params);
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
