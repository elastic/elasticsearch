/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.LegacyTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow;

/**
 * This action is used when making a REST request to the inference API. The transport handler
 * will then look at the task type in the params (or retrieve it from the persisted model if it wasn't
 * included in the params) to determine where this request should be routed. If the task type is chat completion
 * then it will be routed to the unified chat completion handler by creating the {@link UnifiedCompletionAction}.
 * If not, it will be passed along to {@link InferenceAction}.
 */
public class InferenceActionProxy extends ActionType<InferenceActionProxy.Response> {

    public static final InferenceActionProxy INSTANCE = new InferenceActionProxy();
    public static final String NAME = "cluster:monitor/xpack/inference";

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
            // TODO do we need any validation here?
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
                && contentType == request.contentType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskType, inferenceEntityId, content, contentType);
        }
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {

        private final InferenceServiceResults results;
        private final boolean isStreaming;
        private final Flow.Publisher<ChunkedToXContent> publisher;

        public Response(InferenceServiceResults results) {
            this.results = results;
            this.isStreaming = false;
            this.publisher = null;
        }

        public Response(InferenceServiceResults results, Flow.Publisher<ChunkedToXContent> publisher) {
            this.results = results;
            this.isStreaming = true;
            this.publisher = publisher;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
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
        public Flow.Publisher<ChunkedToXContent> publisher() {
            assert isStreaming() : "this should only be called after isStreaming() verifies this object is non-null";
            return publisher;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeNamedWriteable(results);
            } else {
                out.writeNamedWriteable(results.transformToLegacyFormat().get(0));
            }
            // streaming isn't supported via Writeable yet
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
