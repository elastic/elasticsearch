/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.InferenceContext;

import java.io.IOException;
import java.util.Objects;

public class EmbeddingAction extends ActionType<InferenceAction.Response> {
    public static final EmbeddingAction INSTANCE = new EmbeddingAction();
    public static final String NAME = "cluster:internal/xpack/inference/embedding";

    public EmbeddingAction() {
        super(NAME);
    }

    public static class Request extends BaseInferenceActionRequest {
        public static Request parseRequest(
            String inferenceEntityId,
            TaskType taskType,
            TimeValue timeout,
            InferenceContext context,
            XContentParser parser
        ) throws IOException {
            var embeddingRequest = EmbeddingRequest.PARSER.apply(parser, null);
            return new Request(inferenceEntityId, taskType, embeddingRequest, context, timeout);
        }

        private final String inferenceEntityId;
        private final TaskType taskType;
        private final EmbeddingRequest embeddingRequest;
        private final TimeValue timeout;

        public Request(String inferenceEntityId, TaskType taskType, EmbeddingRequest embeddingRequest, TimeValue timeout) {
            this(inferenceEntityId, taskType, embeddingRequest, InferenceContext.EMPTY_INSTANCE, timeout);
        }

        public Request(
            String inferenceEntityId,
            TaskType taskType,
            EmbeddingRequest embeddingRequest,
            InferenceContext context,
            TimeValue timeout
        ) {
            super(context);
            this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
            this.taskType = Objects.requireNonNull(taskType);
            this.embeddingRequest = Objects.requireNonNull(embeddingRequest);
            this.timeout = Objects.requireNonNull(timeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.taskType = TaskType.fromStream(in);
            this.embeddingRequest = new EmbeddingRequest(in);
            this.timeout = in.readTimeValue();
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public EmbeddingRequest getEmbeddingRequest() {
            return embeddingRequest;
        }

        public boolean isStreaming() {
            // streaming is not supported for the EMBEDDING task
            return false;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (embeddingRequest == null || embeddingRequest.inputs() == null) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [inputs] cannot be null");
                return e;
            }

            if (embeddingRequest.inputs().isEmpty()) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [inputs] cannot be an empty array");
                return e;
            }

            if (taskType.isAnyOrSame(TaskType.EMBEDDING) == false) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [taskType] must be [embedding]");
                return e;
            }

            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
            taskType.writeTo(out);
            embeddingRequest.writeTo(out);
            out.writeTimeValue(timeout);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return super.equals(o)
                && Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && taskType == request.taskType
                && Objects.equals(embeddingRequest, request.embeddingRequest)
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), inferenceEntityId, taskType, embeddingRequest, timeout);
        }
    }

}
