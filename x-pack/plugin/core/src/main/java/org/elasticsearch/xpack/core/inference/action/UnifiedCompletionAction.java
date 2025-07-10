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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
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
            TimeValue timeout,
            InferenceContext context,
            XContentParser parser
        ) throws IOException {
            var unifiedRequest = UnifiedCompletionRequest.PARSER.apply(parser, null);
            return new Request(inferenceEntityId, taskType, unifiedRequest, context, timeout);
        }

        private final String inferenceEntityId;
        private final TaskType taskType;
        private final UnifiedCompletionRequest unifiedCompletionRequest;
        private final TimeValue timeout;

        public Request(String inferenceEntityId, TaskType taskType, UnifiedCompletionRequest unifiedCompletionRequest, TimeValue timeout) {
            this(inferenceEntityId, taskType, unifiedCompletionRequest, InferenceContext.EMPTY_INSTANCE, timeout);
        }

        public Request(
            String inferenceEntityId,
            TaskType taskType,
            UnifiedCompletionRequest unifiedCompletionRequest,
            InferenceContext context,
            TimeValue timeout
        ) {
            super(context);
            this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
            this.taskType = Objects.requireNonNull(taskType);
            this.unifiedCompletionRequest = Objects.requireNonNull(unifiedCompletionRequest);
            this.timeout = Objects.requireNonNull(timeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.taskType = TaskType.fromStream(in);
            this.unifiedCompletionRequest = new UnifiedCompletionRequest(in);
            this.timeout = in.readTimeValue();
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

        /**
         * The Unified API only supports streaming so we always return true here.
         * @return true
         */
        public boolean isStreaming() {
            return true;
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
            out.writeTimeValue(timeout);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && taskType == request.taskType
                && Objects.equals(unifiedCompletionRequest, request.unifiedCompletionRequest)
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceEntityId, taskType, unifiedCompletionRequest, timeout);
        }
    }

}
