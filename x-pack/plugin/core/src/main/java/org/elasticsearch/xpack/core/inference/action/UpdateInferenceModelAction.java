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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.ModelConfigurations.SERVICE_SETTINGS;
import static org.elasticsearch.inference.ModelConfigurations.TASK_SETTINGS;

public class UpdateInferenceModelAction extends ActionType<UpdateInferenceModelAction.Response> {

    public static final UpdateInferenceModelAction INSTANCE = new UpdateInferenceModelAction();
    public static final String NAME = "cluster:admin/xpack/inference/update";

    public UpdateInferenceModelAction() {
        super(NAME);
    }

    public record Settings(
        @Nullable Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable TaskType taskType
    ) {}

    public static class Request extends AcknowledgedRequest<Request> {

        private final String inferenceEntityId;
        private final BytesReference content;
        private final XContentType contentType;
        private final TaskType taskType;
        private Settings settings;

        public Request(String inferenceEntityId, BytesReference content, XContentType contentType, TaskType taskType, TimeValue timeout) {
            super(timeout, DEFAULT_ACK_TIMEOUT);
            this.inferenceEntityId = inferenceEntityId;
            this.content = content;
            this.contentType = contentType;
            this.taskType = taskType;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.content = in.readBytesReference();
            this.taskType = TaskType.fromStream(in);
            this.contentType = in.readEnum(XContentType.class);
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public TaskType getTaskType() {
            return taskType;
        }

        /**
         * The body of the request.
         * For in-cluster models, this is expected to contain some of the following:
         * "number_of_allocations": `an integer`
         *
         * For third-party services, this is expected to contain:
         *  "service_settings": {
         *      "api_key": `a string` // service settings can only contain an api key
         *  }
         *  "task_settings": { a map of settings }
         *
         */
        public BytesReference getContent() {
            return content;
        }

        /**
         * The body of the request as a map.
         * The map is validated such that only allowed fields are present.
         * If any fields in the body are not on the allow list, this function will throw an exception.
         */
        public Settings getContentAsSettings() {
            if (settings == null) { // settings is deterministic on content, so we only need to compute it once
                Map<String, Object> unvalidatedMap = XContentHelper.convertToMap(content, false, contentType).v2();
                Map<String, Object> serviceSettings = new HashMap<>();
                Map<String, Object> taskSettings = new HashMap<>();
                TaskType taskType = null;

                if (unvalidatedMap.isEmpty()) {
                    throw new ElasticsearchStatusException("Request body is empty", RestStatus.BAD_REQUEST);
                }

                if (unvalidatedMap.containsKey("task_type")) {
                    if (unvalidatedMap.get("task_type") instanceof String taskTypeString) {
                        taskType = TaskType.fromStringOrStatusException(taskTypeString);
                    } else {
                        throw new ElasticsearchStatusException(
                            "Failed to parse [task_type] in update request [{}]",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            unvalidatedMap.toString()
                        );
                    }
                    unvalidatedMap.remove("task_type");
                }

                if (unvalidatedMap.containsKey(SERVICE_SETTINGS)) {
                    if (unvalidatedMap.get(SERVICE_SETTINGS) instanceof Map<?, ?> tempMap) {
                        for (Map.Entry<?, ?> entry : (tempMap).entrySet()) {
                            if (entry.getKey() instanceof String key && entry.getValue() instanceof Object value) {
                                serviceSettings.put(key, value);
                            } else {
                                throw new ElasticsearchStatusException(
                                    "Failed to parse update request [{}]",
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    unvalidatedMap.toString()
                                );
                            }
                        }
                        unvalidatedMap.remove(SERVICE_SETTINGS);
                    } else {
                        throw new ElasticsearchStatusException(
                            "Unable to parse service settings in the request [{}]",
                            RestStatus.BAD_REQUEST,
                            unvalidatedMap.toString()
                        );
                    }
                }

                if (unvalidatedMap.containsKey(TASK_SETTINGS)) {
                    if (unvalidatedMap.get(TASK_SETTINGS) instanceof Map<?, ?> tempMap) {
                        for (Map.Entry<?, ?> entry : (tempMap).entrySet()) {
                            if (entry.getKey() instanceof String key && entry.getValue() instanceof Object value) {
                                taskSettings.put(key, value);
                            } else {
                                throw new ElasticsearchStatusException(
                                    "Failed to parse update request [{}]",
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    unvalidatedMap.toString()
                                );
                            }
                        }
                        unvalidatedMap.remove(TASK_SETTINGS);
                    } else {
                        throw new ElasticsearchStatusException(
                            "Unable to parse task settings in the request [{}]",
                            RestStatus.BAD_REQUEST,
                            unvalidatedMap.toString()
                        );
                    }
                }

                if (unvalidatedMap.isEmpty() == false) {
                    throw new ElasticsearchStatusException(
                        "Request contained fields which cannot be updated, remove these fields and try again [{}]",
                        RestStatus.BAD_REQUEST,
                        unvalidatedMap.toString()
                    );
                }

                this.settings = new Settings(
                    serviceSettings.isEmpty() == false ? Collections.unmodifiableMap(serviceSettings) : null,
                    taskSettings.isEmpty() == false ? Collections.unmodifiableMap(taskSettings) : null,
                    taskType
                );
            }
            return this.settings;
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
            return Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && Objects.equals(content, request.content)
                && contentType == request.contentType
                && taskType == request.taskType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceEntityId, content, contentType, taskType);
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
