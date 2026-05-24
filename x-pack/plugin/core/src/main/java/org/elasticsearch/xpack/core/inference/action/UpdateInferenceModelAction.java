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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.ModelConfigurations.SERVICE_SETTINGS;
import static org.elasticsearch.inference.ModelConfigurations.TASK_SETTINGS;
import static org.elasticsearch.ingest.IngestDocument.deepCopyMap;

public class UpdateInferenceModelAction extends ActionType<UpdateInferenceModelAction.Response> {

    public static final UpdateInferenceModelAction INSTANCE = new UpdateInferenceModelAction();
    public static final String NAME = "cluster:admin/xpack/inference/update";

    public UpdateInferenceModelAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String inferenceEntityId;
        private final BytesReference content;
        private final XContentType contentType;
        private final TaskType taskType;
        /**
         * Encapsulates the lazy, validated parse of {@link #content} so the three body sections
         * (service_settings, task_settings, task_type) share a single XContent pass.
         */
        private final CachedParsedSettings cachedParsedSettings;

        public Request(String inferenceEntityId, BytesReference content, XContentType contentType, TaskType taskType, TimeValue timeout) {
            super(timeout, DEFAULT_ACK_TIMEOUT);
            this.inferenceEntityId = inferenceEntityId;
            this.content = content;
            this.contentType = contentType;
            this.taskType = taskType;
            this.cachedParsedSettings = new CachedParsedSettings();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.taskType = TaskType.fromStream(in);
            this.content = in.readBytesReference();
            this.contentType = in.readEnum(XContentType.class);
            this.cachedParsedSettings = new CachedParsedSettings();
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
         * Returns the {@code service_settings} block from the request body as a fresh, modifiable
         * deep copy of the parsed content (or {@code null} if the body did not include any).
         *
         * <p>Each invocation returns a new map instance so callers (notably the
         * {@code *Settings.update*} parsers in the inference services) may freely call
         * {@code remove(...)} at any depth without risking corruption of the cached state or
         * interference between consumers reading the same body.
         */
        @Nullable
        public Map<String, Object> getServiceSettings() {
            return cachedParsedSettings.getServiceSettings();
        }

        /**
         * Returns the {@code task_settings} block from the request body as a fresh, modifiable
         * deep copy of the parsed content (or {@code null} if the body did not include any).
         * Same per-call deep-copy semantics as {@link #getServiceSettings()}.
         */
        @Nullable
        public Map<String, Object> getTaskSettings() {
            return cachedParsedSettings.getTaskSettings();
        }

        /**
         * Returns the {@code task_type} declared in the request body, or {@code null} if it was
         * not present. Distinct from {@link #getTaskType()}, which returns the task type taken
         * from the request URL.
         */
        @Nullable
        public TaskType getBodyTaskType() {
            return cachedParsedSettings.getBodyTaskType();
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

        /**
         * Lazy holder for the parsed and validated body of the request. Kept as a non-static
         * inner class so it can read {@link Request#content} / {@link Request#contentType}
         * directly from the enclosing instance without duplicating those fields.
         *
         * <p>{@link XContentHelper#convertToMap} is non-trivial, so all three body sections share
         * one parse pass. The cached maps are kept private; each accessor returns a fresh deep
         * copy so consumers may freely call {@code remove(...)} at any depth without risking
         * corruption of the cache or interference between consumers reading the same body.
         */
        private final class CachedParsedSettings {

            private boolean parsed = false;
            @Nullable
            private Map<String, Object> serviceSettings;
            @Nullable
            private Map<String, Object> taskSettings;
            @Nullable
            private TaskType bodyTaskType;

            @Nullable
            Map<String, Object> getServiceSettings() {
                parseContentIfNeeded();
                return serviceSettings != null ? deepCopyMap(serviceSettings) : null;
            }

            @Nullable
            Map<String, Object> getTaskSettings() {
                parseContentIfNeeded();
                return taskSettings != null ? deepCopyMap(taskSettings) : null;
            }

            @Nullable
            TaskType getBodyTaskType() {
                parseContentIfNeeded();
                return bodyTaskType;
            }

            private void parseContentIfNeeded() {
                if (parsed) {
                    return;
                }
                Map<String, Object> unvalidatedMap = XContentHelper.convertToMap(content, false, contentType).v2();

                if (unvalidatedMap.isEmpty()) {
                    throw new ElasticsearchStatusException("Request body is empty", RestStatus.BAD_REQUEST);
                }

                TaskType parsedBodyTaskType = null;
                if (unvalidatedMap.containsKey(TaskType.NAME)) {
                    if (unvalidatedMap.get(TaskType.NAME) instanceof String taskTypeString) {
                        parsedBodyTaskType = TaskType.fromStringOrStatusException(taskTypeString);
                    } else {
                        throw new ElasticsearchStatusException(
                            "Failed to parse [{}] in update request [{}]",
                            RestStatus.BAD_REQUEST,
                            TaskType.NAME,
                            unvalidatedMap.toString()
                        );
                    }
                    unvalidatedMap.remove(TaskType.NAME);
                }

                Map<String, Object> parsedServiceSettings = new HashMap<>();
                moveFieldToMap(unvalidatedMap, parsedServiceSettings, SERVICE_SETTINGS);

                Map<String, Object> parsedTaskSettings = new HashMap<>();
                moveFieldToMap(unvalidatedMap, parsedTaskSettings, TASK_SETTINGS);

                if (unvalidatedMap.isEmpty() == false) {
                    throw new ElasticsearchStatusException(
                        "Request contained fields which cannot be updated, remove these fields and try again [{}]",
                        RestStatus.BAD_REQUEST,
                        unvalidatedMap.toString()
                    );
                }

                this.serviceSettings = parsedServiceSettings.isEmpty() == false ? parsedServiceSettings : null;
                this.taskSettings = parsedTaskSettings.isEmpty() == false ? parsedTaskSettings : null;
                this.bodyTaskType = parsedBodyTaskType;
                this.parsed = true;
            }

            /**
             * If {@code sourceMap} contains an entry for {@code fieldName} whose value is itself a
             * {@code Map<String, ?>}, copies the entries into {@code destinationMap} and removes
             * the entry from {@code sourceMap}. Throws if the value is present but is not a map,
             * or if any key inside the inner map is not a {@link String}.
             */
            private static void moveFieldToMap(Map<String, Object> sourceMap, Map<String, Object> destinationMap, String fieldName) {
                if (sourceMap.containsKey(fieldName) == false) {
                    return;
                }
                if (sourceMap.get(fieldName) instanceof Map<?, ?> tempMap) {
                    for (Map.Entry<?, ?> entry : tempMap.entrySet()) {
                        if (entry.getKey() instanceof String key) {
                            destinationMap.put(key, entry.getValue());
                        } else {
                            throw new ElasticsearchStatusException(
                                "Failed to parse update request [{}]",
                                RestStatus.BAD_REQUEST,
                                sourceMap.toString()
                            );
                        }
                    }
                    sourceMap.remove(fieldName);
                } else {
                    throw new ElasticsearchStatusException(
                        "Unable to parse [{}] in the request [{}]",
                        RestStatus.BAD_REQUEST,
                        fieldName,
                        sourceMap.toString()
                    );
                }
            }
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final ModelConfigurations model;

        public Response(ModelConfigurations model) {
            this.model = model;
        }

        public Response(StreamInput in) throws IOException {
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
