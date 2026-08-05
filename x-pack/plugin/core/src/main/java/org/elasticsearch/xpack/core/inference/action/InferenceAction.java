/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.InferenceContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

import static org.elasticsearch.core.Strings.format;

public class InferenceAction extends ActionType<InferenceAction.Response> {

    public static final InferenceAction INSTANCE = new InferenceAction();
    public static final String NAME = "cluster:internal/xpack/inference";

    public InferenceAction() {
        super(NAME);
    }

    public static class Request extends BaseInferenceActionRequest {

        public static final ParseField INPUT = new ParseField("input");
        public static final ParseField INPUT_TYPE = new ParseField("input_type");
        public static final ParseField TASK_SETTINGS = new ParseField("task_settings");
        public static final ParseField TIMEOUT = new ParseField("timeout");

        public static Builder builder(String inferenceEntityId, TaskType taskType) {
            return new Builder().setInferenceEntityId(inferenceEntityId).setTaskType(taskType);
        }

        static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME, Request.Builder::new);
        static {
            PARSER.declareStringArray(Request.Builder::setInput, INPUT);
            PARSER.declareString(Request.Builder::setInputType, INPUT_TYPE);
            PARSER.declareObject(Request.Builder::setTaskSettings, (p, c) -> p.mapOrdered(), TASK_SETTINGS);
            PARSER.declareString(Builder::setInferenceTimeout, TIMEOUT);
        }

        public static Builder parseRequest(String inferenceEntityId, TaskType taskType, InferenceContext context, XContentParser parser)
            throws IOException {
            Request.Builder builder = PARSER.apply(parser, null);
            builder.setInferenceEntityId(inferenceEntityId);
            builder.setTaskType(taskType);
            builder.setContext(context);
            return builder;
        }

        private static final TransportVersion RERANK_COMMON_OPTIONS_ADDED = TransportVersion.fromName("rerank_common_options_added");
        private static final TransportVersion RERANK_MOVED_FROM_INFERENCE_ACTION = TransportVersion.fromName(
            "inference_rerank_moved_from_inference_action"
        );
        /**
         * A set of the {@link TaskType} values which can use {@link InferenceAction}. For {@link TaskType} not included here, please use
         * {@link UnifiedCompletionAction}, {@link EmbeddingAction} or {@link RerankAction} as appropriate
         */
        public static final EnumSet<TaskType> SUPPORTED_INFERENCE_ACTION_TASK_TYPES = EnumSet.of(
            TaskType.TEXT_EMBEDDING,
            TaskType.SPARSE_EMBEDDING,
            TaskType.COMPLETION,
            TaskType.ANY
        );

        private final TaskType taskType;
        private final String inferenceEntityId;
        private final List<String> input;
        private final Map<String, Object> taskSettings;
        private final InputType inputType;
        private final TimeValue inferenceTimeout;
        private final boolean stream;

        /**
         * Constructor that uses an empty {@link InferenceContext}
         *
         * @param taskType          the {@link TaskType} of the inference request. May be {@link TaskType#ANY}, which will result in the
         *                          task type being determined after parsing the stored model
         * @param inferenceEntityId the endpoint ID
         * @param input             the inputs to use for the inference request
         * @param taskSettings      the task settings to use for the inference request
         * @param inputType         the {@link InputType} of the request
         * @param inferenceTimeout  the timeout to use. If null, a placeholder timeout will be used until the appropriate timeout for the
         *                          task type and input type can be determined by the inference service implementation
         * @param stream            whether the request should use streaming
         */
        public Request(
            TaskType taskType,
            String inferenceEntityId,
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            @Nullable TimeValue inferenceTimeout,
            boolean stream
        ) {
            this(taskType, inferenceEntityId, input, taskSettings, inputType, inferenceTimeout, stream, InferenceContext.EMPTY_INSTANCE);
        }

        /**
         * Constructor that allows an {@link InferenceContext} to be specified
         *
         * @param taskType          the {@link TaskType} of the inference request. May be {@link TaskType#ANY}, which will result in the
         *                          task type being determined after parsing the stored model
         * @param inferenceEntityId the endpoint ID
         * @param input             the inputs to use for the inference request
         * @param taskSettings      the task settings to use for the inference request
         * @param inputType         the {@link InputType} of the request
         * @param inferenceTimeout  the timeout to use. If null, a placeholder timeout will be used until the appropriate timeout for the
         *                          task type and input type can be determined by the inference service implementation
         * @param stream            whether the request should use streaming
         * @param context           the {@link InferenceContext} to use, if {@code null} then an empty context will be used
         */
        public Request(
            TaskType taskType,
            String inferenceEntityId,
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            @Nullable TimeValue inferenceTimeout,
            boolean stream,
            InferenceContext context
        ) {
            super(context);
            this.taskType = taskType;
            this.inferenceEntityId = inferenceEntityId;
            this.input = input;
            this.taskSettings = taskSettings;
            this.inputType = inputType;
            this.inferenceTimeout = Objects.requireNonNullElse(inferenceTimeout, TIMEOUT_NOT_DETERMINED);
            this.stream = stream;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.taskType = TaskType.fromStream(in);
            this.inferenceEntityId = in.readString();
            this.input = in.readStringCollectionAsList();
            this.taskSettings = in.readGenericMap();
            this.inputType = in.readEnum(InputType.class);
            if (in.getTransportVersion().supports(RERANK_MOVED_FROM_INFERENCE_ACTION) == false) {
                in.readOptionalString();
            }
            this.inferenceTimeout = in.readTimeValue();

            if (in.getTransportVersion().supports(RERANK_MOVED_FROM_INFERENCE_ACTION) == false) {
                if (in.getTransportVersion().supports(RERANK_COMMON_OPTIONS_ADDED)) {
                    in.readOptionalBoolean();
                    in.readOptionalInt();
                }
            }

            // streaming is not supported yet for transport traffic
            this.stream = false;
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public List<String> getInput() {
            return input;
        }

        public Map<String, Object> getTaskSettings() {
            return taskSettings;
        }

        public InputType getInputType() {
            return inputType;
        }

        public TimeValue getInferenceTimeout() {
            return inferenceTimeout;
        }

        public boolean isStreaming() {
            return stream;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (SUPPORTED_INFERENCE_ACTION_TASK_TYPES.contains(taskType) == false) {
                var e = new ActionRequestValidationException();
                e.addValidationError(
                    Strings.format(
                        "Task type [%s] cannot be used with InferenceAction.Request. Supported task types are %s",
                        taskType,
                        SUPPORTED_INFERENCE_ACTION_TASK_TYPES
                    )
                );
                return e;
            }

            if (input == null) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [input] cannot be null");
                return e;
            }

            if (input.isEmpty()) {
                var e = new ActionRequestValidationException();
                e.addValidationError("Field [input] cannot be an empty array");
                return e;
            }

            if (taskType.equals(TaskType.TEXT_EMBEDDING) == false
                && taskType.equals(TaskType.ANY) == false
                && (inputType != null && InputType.isInternalTypeOrUnspecified(inputType) == false)) {
                var e = new ActionRequestValidationException();
                e.addValidationError(format("Field [input_type] cannot be specified for task type [%s]", taskType));
                return e;
            }

            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            taskType.writeTo(out);
            out.writeString(inferenceEntityId);
            out.writeStringCollection(input);
            out.writeGenericMap(taskSettings);
            out.writeEnum(inputType);
            if (out.getTransportVersion().supports(RERANK_MOVED_FROM_INFERENCE_ACTION) == false) {
                out.writeOptionalString(null);
            }
            if (inferenceTimeout.equals(TIMEOUT_NOT_DETERMINED)
                && out.getTransportVersion().supports(INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED) == false) {
                out.writeTimeValue(OLD_DEFAULT_TIMEOUT);
            } else {
                out.writeTimeValue(inferenceTimeout);
            }

            if (out.getTransportVersion().supports(RERANK_MOVED_FROM_INFERENCE_ACTION) == false) {
                if (out.getTransportVersion().supports(RERANK_COMMON_OPTIONS_ADDED)) {
                    out.writeOptionalBoolean(null);
                    out.writeOptionalInt(null);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            Request request = (Request) o;
            return stream == request.stream
                && taskType == request.taskType
                && Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && Objects.equals(input, request.input)
                && Objects.equals(taskSettings, request.taskSettings)
                && inputType == request.inputType
                && Objects.equals(inferenceTimeout, request.inferenceTimeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), taskType, inferenceEntityId, input, taskSettings, inputType, inferenceTimeout, stream);
        }

        public static class Builder {

            private TaskType taskType;
            private String inferenceEntityId;
            private List<String> input;
            private InputType inputType = InputType.UNSPECIFIED;
            private Map<String, Object> taskSettings = Map.of();
            private TimeValue timeout = TIMEOUT_NOT_DETERMINED;
            private boolean stream = false;
            private InferenceContext context = InferenceContext.EMPTY_INSTANCE;

            private Builder() {}

            public Builder setInferenceEntityId(String inferenceEntityId) {
                this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
                return this;
            }

            public Builder setTaskType(TaskType taskType) {
                this.taskType = taskType;
                return this;
            }

            public Builder setInput(List<String> input) {
                this.input = input;
                return this;
            }

            public Builder setInputType(InputType inputType) {
                this.inputType = inputType;
                return this;
            }

            public Builder setInputType(String inputType) {
                this.inputType = InputType.fromRestString(inputType);
                return this;
            }

            public Builder setTaskSettings(Map<String, Object> taskSettings) {
                this.taskSettings = taskSettings;
                return this;
            }

            public Builder setInferenceTimeout(TimeValue inferenceTimeout) {
                this.timeout = inferenceTimeout;
                return this;
            }

            private Builder setInferenceTimeout(String inferenceTimeout) {
                return setInferenceTimeout(TimeValue.parseTimeValue(inferenceTimeout, TIMEOUT.getPreferredName()));
            }

            public Builder setStream(boolean stream) {
                this.stream = stream;
                return this;
            }

            /**
             * Sets the {@link InferenceContext} for this request. If not set, an empty context will be used.
             * If the context is set to {@code null}, an empty context will also be used.
             * @param context the context to set
             */
            public Builder setContext(InferenceContext context) {
                this.context = Objects.requireNonNullElse(context, InferenceContext.EMPTY_INSTANCE);
                return this;
            }

            public Request build() {
                return new Request(taskType, inferenceEntityId, input, taskSettings, inputType, timeout, stream, context);
            }
        }

        public String toString() {
            return "InferenceAction.Request(taskType="
                + this.getTaskType()
                + ", inferenceEntityId="
                + this.getInferenceEntityId()
                + ", input="
                + this.getInput()
                + ", taskSettings="
                + this.getTaskSettings()
                + ", inputType="
                + this.getInputType()
                + ", timeout="
                + this.getInferenceTimeout()
                + ", context="
                + this.getContext()
                + ")";
        }
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {

        private final InferenceServiceResults results;
        private final boolean isStreaming;
        private final Flow.Publisher<InferenceServiceResults.Result> publisher;

        public Response(InferenceServiceResults results) {
            this.results = results;
            this.isStreaming = false;
            this.publisher = null;
        }

        public Response(InferenceServiceResults results, Flow.Publisher<InferenceServiceResults.Result> publisher) {
            this.results = results;
            this.isStreaming = true;
            this.publisher = publisher;
        }

        public Response(StreamInput in) throws IOException {
            this.results = in.readNamedWriteable(InferenceServiceResults.class);
            // streaming isn't supported via Writeable yet
            this.isStreaming = false;
            this.publisher = null;
        }

        public InferenceServiceResults getResults() {
            return results;
        }

        /**
         * Returns {@code true} if these results are streamed as chunks, or {@code false} if these results contain the entire payload.
         * Currently set to false while it is being implemented.
         */
        public boolean isStreaming() {
            return isStreaming;
        }

        /**
         * When {@link #isStreaming()} is {@code true}, the RestHandler will subscribe to this publisher.
         * When the RestResponse is finished with the current chunk, it will request the next chunk using the subscription.
         * If the RestResponse is closed, it will cancel the subscription.
         */
        public Flow.Publisher<InferenceServiceResults.Result> publisher() {
            assert isStreaming() : "this should only be called after isStreaming() verifies this object is non-null";
            return publisher;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // streaming isn't supported via Writeable yet
            out.writeNamedWriteable(results);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                results.toXContentChunked(params),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(results, response.results);
        }

        @Override
        public int hashCode() {
            return Objects.hash(results);
        }
    }
}
