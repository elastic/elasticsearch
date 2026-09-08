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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.InferenceContext;

import java.io.IOException;
import java.util.Objects;

public class UnifiedCompletionAction extends ActionType<InferenceAction.Response> {
    public static final UnifiedCompletionAction INSTANCE = new UnifiedCompletionAction();
    public static final String NAME = "cluster:internal/xpack/inference/unified";

    public UnifiedCompletionAction() {
        super(NAME);
    }

    public static class Request extends BaseInferenceActionRequest {
        public static Request parseRequest(
            String inferenceEntityId,
            TaskType taskType,
            boolean stream,
            TimeValue timeout,
            InferenceContext context,
            XContentParser parser
        ) throws IOException {
            var unifiedRequest = UnifiedCompletionRequestBody.PARSER.apply(parser, null);
            return new Request(inferenceEntityId, taskType, unifiedRequest, context, stream, timeout);
        }

        private final String inferenceEntityId;
        private final TaskType taskType;
        private final UnifiedCompletionRequestBody unifiedCompletionRequestBody;
        private final boolean stream;
        private final TimeValue timeout;

        public Request(
            String inferenceEntityId,
            TaskType taskType,
            UnifiedCompletionRequestBody unifiedCompletionRequestBody,
            @Nullable TimeValue timeout
        ) {
            this(inferenceEntityId, taskType, unifiedCompletionRequestBody, InferenceContext.EMPTY_INSTANCE, true, timeout);
        }

        public Request(
            String inferenceEntityId,
            TaskType taskType,
            UnifiedCompletionRequestBody unifiedCompletionRequestBody,
            InferenceContext context,
            boolean stream,
            @Nullable TimeValue timeout
        ) {
            super(context);
            this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
            this.taskType = Objects.requireNonNull(taskType);
            this.unifiedCompletionRequestBody = Objects.requireNonNull(unifiedCompletionRequestBody);
            this.stream = stream;
            this.timeout = Objects.requireNonNullElse(timeout, TIMEOUT_NOT_DETERMINED);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.taskType = TaskType.fromStream(in);
            this.unifiedCompletionRequestBody = new UnifiedCompletionRequestBody(in);
            this.timeout = in.readTimeValue();
            this.stream = in.readBoolean();
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public UnifiedCompletionRequestBody getUnifiedCompletionRequest() {
            return unifiedCompletionRequestBody;
        }

        public boolean isStreaming() {
            return stream;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (unifiedCompletionRequestBody == null || unifiedCompletionRequestBody.messages() == null) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [messages] cannot be null");
                return e;
            }

            if (unifiedCompletionRequestBody.messages().isEmpty()) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [messages] cannot be an empty array");
                return e;
            }

            if (taskType.isAnyOrSame(TaskType.CHAT_COMPLETION) == false) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [taskType] must be [chat_completion]");
                return e;
            }

            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
            taskType.writeTo(out);
            unifiedCompletionRequestBody.writeTo(out);
            if (timeout.equals(TIMEOUT_NOT_DETERMINED)
                && out.getTransportVersion().supports(INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED) == false) {
                out.writeTimeValue(OLD_DEFAULT_TIMEOUT);
            } else {
                out.writeTimeValue(timeout);
            }
            out.writeBoolean(stream);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            var request = (Request) o;
            return super.equals(o)
                && stream == request.stream
                && Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && taskType == request.taskType
                && Objects.equals(unifiedCompletionRequestBody, request.unifiedCompletionRequestBody)
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), inferenceEntityId, taskType, unifiedCompletionRequestBody, stream, timeout);
        }
    }

}
