/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.xpack.core.inference.InferenceContext;

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
        private final InferenceContext context;

        public Request(
            TaskType taskType,
            String inferenceEntityId,
            BytesReference content,
            XContentType contentType,
            TimeValue timeout,
            boolean stream,
            InferenceContext context
        ) {
            this.taskType = taskType;
            this.inferenceEntityId = inferenceEntityId;
            this.content = content;
            this.contentType = contentType;
            this.timeout = timeout;
            this.stream = stream;
            this.context = context;
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

            if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_CONTEXT)
                || in.getTransportVersion().isPatchFrom(TransportVersions.INFERENCE_CONTEXT_8_X)) {
                this.context = new InferenceContext(in);
            } else {
                this.context = InferenceContext.EMPTY_INSTANCE;
            }
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

        public InferenceContext getContext() {
            return context;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            taskType.writeTo(out);
            out.writeString(inferenceEntityId);
            out.writeBytesReference(content);
            XContentHelper.writeTo(out, contentType);
            out.writeTimeValue(timeout);

            if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_CONTEXT)
                || out.getTransportVersion().isPatchFrom(TransportVersions.INFERENCE_CONTEXT_8_X)) {
                context.writeTo(out);
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
                && contentType == request.contentType
                && timeout == request.timeout
                && stream == request.stream
                && context == request.context;
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskType, inferenceEntityId, content, contentType, timeout, stream, context);
        }
    }
}
