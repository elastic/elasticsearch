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
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.InferenceContext;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_NON_STREAMING_ADDED;

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
            var unifiedRequest = UnifiedCompletionRequest.PARSER.apply(parser, null);
            return new Request(inferenceEntityId, taskType, unifiedRequest, context, stream, timeout);
        }

        private final String inferenceEntityId;
        private final TaskType taskType;
        private final UnifiedCompletionRequest unifiedCompletionRequest;
        private final boolean stream;
        private final TimeValue timeout;

        public Request(
            String inferenceEntityId,
            TaskType taskType,
            UnifiedCompletionRequest unifiedCompletionRequest,
            @Nullable TimeValue timeout
        ) {
            this(inferenceEntityId, taskType, unifiedCompletionRequest, InferenceContext.EMPTY_INSTANCE, true, timeout);
        }

        public Request(
            String inferenceEntityId,
            TaskType taskType,
            UnifiedCompletionRequest unifiedCompletionRequest,
            InferenceContext context,
            boolean stream,
            @Nullable TimeValue timeout
        ) {
            super(context);
            this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
            this.taskType = Objects.requireNonNull(taskType);
            this.unifiedCompletionRequest = Objects.requireNonNull(unifiedCompletionRequest);
            this.stream = stream;
            this.timeout = Objects.requireNonNullElse(timeout, TIMEOUT_NOT_DETERMINED);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.taskType = TaskType.fromStream(in);
            this.unifiedCompletionRequest = new UnifiedCompletionRequest(in);
            this.timeout = in.readTimeValue();
            if (in.getTransportVersion().supports(CHAT_COMPLETION_NON_STREAMING_ADDED)) {
                this.stream = in.readBoolean();
            } else {
                this.stream = true;
            }
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public UnifiedCompletionRequest getUnifiedCompletionRequest() {
            return unifiedCompletionRequest;
        }

        public boolean isStreaming() {
            return stream;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (unifiedCompletionRequest == null || unifiedCompletionRequest.messages() == null) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [messages] cannot be null");
                return e;
            }

            if (unifiedCompletionRequest.messages().isEmpty()) {
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
            unifiedCompletionRequest.writeTo(out);
            if (timeout.equals(TIMEOUT_NOT_DETERMINED)
                && out.getTransportVersion().supports(INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED) == false) {
                out.writeTimeValue(OLD_DEFAULT_TIMEOUT);
            } else {
                out.writeTimeValue(timeout);
            }
            if (out.getTransportVersion().supports(CHAT_COMPLETION_NON_STREAMING_ADDED)) {
                out.writeBoolean(stream);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            var request = (Request) o;
            return super.equals(o)
                && stream == request.stream
                && Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && taskType == request.taskType
                && Objects.equals(unifiedCompletionRequest, request.unifiedCompletionRequest)
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), inferenceEntityId, taskType, unifiedCompletionRequest, stream, timeout);
        }
    }

}
