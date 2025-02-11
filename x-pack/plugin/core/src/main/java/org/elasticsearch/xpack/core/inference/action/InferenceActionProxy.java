/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

/**
 * This action is used when making a REST request to the inference API. The transport handler
 * will then look at the task type in the params (or retrieve it from the persisted model if it wasn't
 * included in the params) to determine where this request should be routed. If the task type is chat completion
 * then it will be routed to the unified chat completion handler by creating the {@link UnifiedCompletionAction}.
 * If not, it will be passed along to {@link InferenceAction}.
 */
public class InferenceActionProxy extends ActionType<InferenceAction.Response> {
    public static final InferenceActionProxy INSTANCE = new InferenceActionProxy();
    public static final String NAME = "cluster:monitor/xpack/inference/post";

    public InferenceActionProxy() {
        super(NAME);
    }

    public static class Request extends ActionRequest {

        private final TaskType taskType;
        private final String inferenceEntityId;
        private final BytesReference content;
        private final XContentType contentType;
        private final TimeValue timeout;
        private final boolean stream;

        public Request(
            TaskType taskType,
            String inferenceEntityId,
            BytesReference content,
            XContentType contentType,
            TimeValue timeout,
            boolean stream
        ) {
            this.taskType = taskType;
            this.inferenceEntityId = inferenceEntityId;
            this.content = content;
            this.contentType = contentType;
            this.timeout = timeout;
            this.stream = stream;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.taskType = TaskType.fromStream(in);
            this.inferenceEntityId = in.readString();
            this.content = in.readBytesReference();
            this.contentType = in.readEnum(XContentType.class);
            this.timeout = in.readTimeValue();

            // streaming is not supported yet for transport traffic
            this.stream = false;
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

        public XContentType getContentType() {
            return contentType;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public boolean isStreaming() {
            return stream;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
            taskType.writeTo(out);
            out.writeBytesReference(content);
            XContentHelper.writeTo(out, contentType);
            out.writeTimeValue(timeout);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return taskType == request.taskType
                && Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && Objects.equals(content, request.content)
                && contentType == request.contentType
                && timeout == request.timeout
                && stream == request.stream;
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskType, inferenceEntityId, content, contentType, timeout, stream);
        }
    }
}
