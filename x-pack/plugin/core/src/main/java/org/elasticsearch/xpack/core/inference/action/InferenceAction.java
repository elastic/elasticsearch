/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.inference.results.LegacyTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.ArrayList;
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

        public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30);
        public static final ParseField INPUT = new ParseField("input");
        public static final ParseField INPUT_TYPE = new ParseField("input_type");
        public static final ParseField TASK_SETTINGS = new ParseField("task_settings");
        public static final ParseField QUERY = new ParseField("query");
        public static final ParseField RETURN_DOCUMENTS = new ParseField("return_documents");
        public static final ParseField TOP_N = new ParseField("top_n");
        public static final ParseField TIMEOUT = new ParseField("timeout");

        public static Builder builder(String inferenceEntityId, TaskType taskType) {
            return new Builder().setInferenceEntityId(inferenceEntityId).setTaskType(taskType);
        }

        static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME, Request.Builder::new);
        static {
            PARSER.declareStringArray(Request.Builder::setInput, INPUT);
            PARSER.declareString(Request.Builder::setInputType, INPUT_TYPE);
            PARSER.declareObject(Request.Builder::setTaskSettings, (p, c) -> p.mapOrdered(), TASK_SETTINGS);
            PARSER.declareString(Request.Builder::setQuery, QUERY);
            PARSER.declareBoolean(Request.Builder::setReturnDocuments, RETURN_DOCUMENTS);
            PARSER.declareInt(Request.Builder::setTopN, TOP_N);
            PARSER.declareString(Builder::setInferenceTimeout, TIMEOUT);
        }

        private static final EnumSet<InputType> validEnumsBeforeUnspecifiedAdded = EnumSet.of(InputType.INGEST, InputType.SEARCH);
        private static final EnumSet<InputType> validEnumsBeforeClassificationClusteringAdded = EnumSet.range(
            InputType.INGEST,
            InputType.UNSPECIFIED
        );

        public static Builder parseRequest(String inferenceEntityId, TaskType taskType, InferenceContext context, XContentParser parser)
            throws IOException {
            Request.Builder builder = PARSER.apply(parser, null);
            builder.setInferenceEntityId(inferenceEntityId);
            builder.setTaskType(taskType);
            builder.setContext(context);
            return builder;
        }

        private final TaskType taskType;
        private final String inferenceEntityId;
        private final String query;
        private final Boolean returnDocuments;
        private final Integer topN;
        private final List<String> input;
        private final Map<String, Object> taskSettings;
        private final InputType inputType;
        private final TimeValue inferenceTimeout;
        private final boolean stream;

        public Request(
            TaskType taskType,
            String inferenceEntityId,
            String query,
            Boolean returnDocuments,
            Integer topN,
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            TimeValue inferenceTimeout,
            boolean stream
        ) {
            this(
                taskType,
                inferenceEntityId,
                query,
                returnDocuments,
                topN,
                input,
                taskSettings,
                inputType,
                inferenceTimeout,
                stream,
                InferenceContext.EMPTY_INSTANCE
            );
        }

        public Request(
            TaskType taskType,
            String inferenceEntityId,
            String query,
            Boolean returnDocuments,
            Integer topN,
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            TimeValue inferenceTimeout,
            boolean stream,
            InferenceContext context
        ) {
            super(context);
            this.taskType = taskType;
            this.inferenceEntityId = inferenceEntityId;
            this.query = query;
            this.returnDocuments = returnDocuments;
            this.topN = topN;
            this.input = input;
            this.taskSettings = taskSettings;
            this.inputType = inputType;
            this.inferenceTimeout = inferenceTimeout;
            this.stream = stream;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.taskType = TaskType.fromStream(in);
            this.inferenceEntityId = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                this.input = in.readStringCollectionAsList();
            } else {
                this.input = List.of(in.readString());
            }
            this.taskSettings = in.readGenericMap();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                this.inputType = in.readEnum(InputType.class);
            } else {
                this.inputType = InputType.UNSPECIFIED;
            }

            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                this.query = in.readOptionalString();
                this.inferenceTimeout = in.readTimeValue();
            } else {
                this.query = null;
                this.inferenceTimeout = DEFAULT_TIMEOUT;
            }

            if (in.getTransportVersion().onOrAfter(TransportVersions.RERANK_COMMON_OPTIONS_ADDED)
                || in.getTransportVersion().isPatchFrom(TransportVersions.RERANK_COMMON_OPTIONS_ADDED_8_19)) {
                this.returnDocuments = in.readOptionalBoolean();
                this.topN = in.readOptionalInt();
            } else {
                this.returnDocuments = null;
                this.topN = null;
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

        public String getQuery() {
            return query;
        }

        public Boolean getReturnDocuments() {
            return returnDocuments;
        }

        public Integer getTopN() {
            return topN;
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

            if (taskType.equals(TaskType.RERANK)) {
                if (query == null) {
                    var e = new ActionRequestValidationException();
                    e.addValidationError(format("Field [query] cannot be null for task type [%s]", TaskType.RERANK));
                    return e;
                }
                if (query.isEmpty()) {
                    var e = new ActionRequestValidationException();
                    e.addValidationError(format("Field [query] cannot be empty for task type [%s]", TaskType.RERANK));
                    return e;
                }
            } else if (taskType.equals(TaskType.ANY) == false) {
                if (returnDocuments != null) {
                    var e = new ActionRequestValidationException();
                    e.addValidationError(format("Field [return_documents] cannot be specified for task type [%s]", taskType));
                    return e;
                }
                if (topN != null) {
                    var e = new ActionRequestValidationException();
                    e.addValidationError(format("Field [top_n] cannot be specified for task type [%s]", taskType));
                    return e;
                }
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
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeStringCollection(input);
            } else {
                out.writeString(input.get(0));
            }
            out.writeGenericMap(taskSettings);

            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                out.writeEnum(getInputTypeToWrite(inputType, out.getTransportVersion()));
            }

            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeOptionalString(query);
                out.writeTimeValue(inferenceTimeout);
            }

            if (out.getTransportVersion().onOrAfter(TransportVersions.RERANK_COMMON_OPTIONS_ADDED)
                || out.getTransportVersion().isPatchFrom(TransportVersions.RERANK_COMMON_OPTIONS_ADDED_8_19)) {
                out.writeOptionalBoolean(returnDocuments);
                out.writeOptionalInt(topN);
            }
        }

        // default for easier testing
        static InputType getInputTypeToWrite(InputType inputType, TransportVersion version) {
            if (version.before(TransportVersions.V_8_13_0)) {
                if (validEnumsBeforeUnspecifiedAdded.contains(inputType) == false) {
                    return InputType.INGEST;
                } else if (validEnumsBeforeClassificationClusteringAdded.contains(inputType) == false) {
                    return InputType.UNSPECIFIED;
                }
            }

            return inputType;
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
                && Objects.equals(query, request.query)
                && Objects.equals(returnDocuments, request.returnDocuments)
                && Objects.equals(topN, request.topN)
                && Objects.equals(input, request.input)
                && Objects.equals(taskSettings, request.taskSettings)
                && inputType == request.inputType
                && Objects.equals(inferenceTimeout, request.inferenceTimeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                super.hashCode(),
                taskType,
                inferenceEntityId,
                query,
                returnDocuments,
                topN,
                input,
                taskSettings,
                inputType,
                inferenceTimeout,
                stream
            );
        }

        public static class Builder {

            private TaskType taskType;
            private String inferenceEntityId;
            private List<String> input;
            private InputType inputType = InputType.UNSPECIFIED;
            private Map<String, Object> taskSettings = Map.of();
            private String query;
            private Boolean returnDocuments;
            private Integer topN;
            private TimeValue timeout = DEFAULT_TIMEOUT;
            private boolean stream = false;
            private InferenceContext context;

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

            public Builder setQuery(String query) {
                this.query = query;
                return this;
            }

            public Builder setReturnDocuments(Boolean returnDocuments) {
                this.returnDocuments = returnDocuments;
                return this;
            }

            public Builder setTopN(Integer topN) {
                this.topN = topN;
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

            public Builder setContext(InferenceContext context) {
                this.context = context;
                return this;
            }

            public Request build() {
                return new Request(
                    taskType,
                    inferenceEntityId,
                    query,
                    returnDocuments,
                    topN,
                    input,
                    taskSettings,
                    inputType,
                    timeout,
                    stream,
                    context
                );
            }
        }

        public String toString() {
            return "InferenceAction.Request(taskType="
                + this.getTaskType()
                + ", inferenceEntityId="
                + this.getInferenceEntityId()
                + ", query="
                + this.getQuery()
                + ", returnDocuments="
                + this.getReturnDocuments()
                + ", topN="
                + this.getTopN()
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
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                results = in.readNamedWriteable(InferenceServiceResults.class);
            } else {
                // It should only be InferenceResults aka TextEmbeddingResults from ml plugin for
                // hugging face elser and elser
                results = transformToServiceResults(List.of(in.readNamedWriteable(InferenceResults.class)));
            }
            // streaming isn't supported via Writeable yet
            this.isStreaming = false;
            this.publisher = null;
        }

        @SuppressWarnings("deprecation")
        public static InferenceServiceResults transformToServiceResults(List<? extends InferenceResults> parsedResults) {
            if (parsedResults.isEmpty()) {
                throw new ElasticsearchStatusException(
                    "Failed to transform results to response format, expected a non-empty list, please remove and re-add the service",
                    RestStatus.INTERNAL_SERVER_ERROR
                );
            }

            if (parsedResults.get(0) instanceof LegacyTextEmbeddingResults openaiResults) {
                if (parsedResults.size() > 1) {
                    throw new ElasticsearchStatusException(
                        "Failed to transform results to response format, malformed text embedding result,"
                            + " please remove and re-add the service",
                        RestStatus.INTERNAL_SERVER_ERROR
                    );
                }

                return openaiResults.transformToTextEmbeddingResults();
            } else if (parsedResults.get(0) instanceof TextExpansionResults) {
                return transformToSparseEmbeddingResult(parsedResults);
            } else {
                throw new ElasticsearchStatusException(
                    "Failed to transform results to response format, unknown embedding type received,"
                        + " please remove and re-add the service",
                    RestStatus.INTERNAL_SERVER_ERROR
                );
            }
        }

        private static SparseEmbeddingResults transformToSparseEmbeddingResult(List<? extends InferenceResults> parsedResults) {
            List<TextExpansionResults> textExpansionResults = new ArrayList<>(parsedResults.size());

            for (InferenceResults result : parsedResults) {
                if (result instanceof TextExpansionResults textExpansion) {
                    textExpansionResults.add(textExpansion);
                } else {
                    throw new ElasticsearchStatusException(
                        "Failed to transform results to response format, please remove and re-add the service",
                        RestStatus.INTERNAL_SERVER_ERROR
                    );
                }
            }

            return SparseEmbeddingResults.of(textExpansionResults);
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
