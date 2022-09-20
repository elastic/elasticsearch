/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;

import java.io.IOException;
import java.util.Objects;

public class DenseSearchAction extends ActionType<DenseSearchAction.Response> {

    public static final String NAME = "cluster:monitor/xpack/ml/dense_search";

    public static final DenseSearchAction INSTANCE = new DenseSearchAction(NAME);

    private DenseSearchAction(String name) {
        super(name, DenseSearchAction.Response::new);
    }

    public static class Request extends ActionRequest {

        public static final ParseField DEPLOYMENT_ID = new ParseField("deployment_id");
        public static final ParseField QUERY_STRING = new ParseField("query_string");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");
        public static final ParseField KNN = new ParseField("knn");

        static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME, Request.Builder::new);

        static {
            PARSER.declareString(Request.Builder::setDeploymentId, DEPLOYMENT_ID);
            PARSER.declareString(Request.Builder::setQueryString, QUERY_STRING);
            PARSER.declareString(Request.Builder::setTimeout, TIMEOUT);
            PARSER.declareObject(Request.Builder::setUpdate, (p, c) -> TextEmbeddingConfigUpdate.fromXContentStrict(p), INFERENCE_CONFIG);
            PARSER.declareObject(Request.Builder::setKnnSearch, (p, c) -> KnnSearchBuilder.fromXContent(p), KNN);
        }

        private final String queryString;
        private final String deploymentId;
        private final TimeValue inferenceTimeout;
        private final KnnSearchBuilder knnSearchBuilder;
        private final TextEmbeddingConfigUpdate embeddingConfig;

        public Request(StreamInput in) throws IOException {
            super(in);
            queryString = in.readString();
            deploymentId = in.readString();
            inferenceTimeout = in.readOptionalTimeValue();
            knnSearchBuilder = new KnnSearchBuilder(in);
            embeddingConfig = in.readOptionalWriteable(TextEmbeddingConfigUpdate::new);
        }

        Request(
            String queryString,
            String deploymentId,
            KnnSearchBuilder knnSearchBuilder,
            TextEmbeddingConfigUpdate embeddingConfig,
            TimeValue inferenceTimeout
        ) {
            this.queryString = queryString;
            this.deploymentId = deploymentId;
            this.knnSearchBuilder = knnSearchBuilder;
            this.embeddingConfig = embeddingConfig;
            this.inferenceTimeout = inferenceTimeout;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(queryString);
            out.writeString(deploymentId);
            out.writeOptionalTimeValue(inferenceTimeout);
            knnSearchBuilder.writeTo(out);
            out.writeOptionalWriteable(embeddingConfig);
        }

        public String getQueryString() {
            return queryString;
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public TimeValue getInferenceTimeout() {
            return inferenceTimeout;
        }

        public KnnSearchBuilder getKnnSearchBuilder() {
            return knnSearchBuilder;
        }

        public TextEmbeddingConfigUpdate getEmbeddingConfig() {
            return embeddingConfig;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(deploymentId, request.deploymentId)
                && Objects.equals(queryString, request.queryString)
                && Objects.equals(inferenceTimeout, request.inferenceTimeout)
                && Objects.equals(knnSearchBuilder, request.knnSearchBuilder)
                && Objects.equals(embeddingConfig, request.embeddingConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryString, deploymentId, inferenceTimeout, knnSearchBuilder, embeddingConfig);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException error = new ActionRequestValidationException();
            if (queryString == null) {
                error.addValidationError("query_string cannot be null");
            }
            if (deploymentId == null) {
                error.addValidationError("deployment_id cannot be null");
            }
            if (knnSearchBuilder == null) {
                error.addValidationError("knn cannot be null");
            }

            return error.validationErrors().isEmpty() ? null : error;
        }

        public static class Builder {

            private String deploymentId;
            private String queryString;
            private TimeValue timeout;
            private TextEmbeddingConfigUpdate update;
            private KnnSearchBuilder knnSearchBuilder;

            private Builder() {}

            void setDeploymentId(String deploymentId) {
                this.deploymentId = deploymentId;
            }

            void setQueryString(String queryString) {
                this.queryString = queryString;
            }

            void setTimeout(TimeValue timeout) {
                this.timeout = timeout;
            }

            void setTimeout(String timeout) {
                setTimeout(TimeValue.parseTimeValue(timeout, TIMEOUT.getPreferredName()));
            }

            void setUpdate(TextEmbeddingConfigUpdate update) {
                this.update = update;
            }

            void setKnnSearch(KnnSearchBuilder knnSearchBuilder) {
                this.knnSearchBuilder = knnSearchBuilder;
            }

            Request build() {
                return new Request(queryString, deploymentId, knnSearchBuilder, update, timeout);
            }
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final TimeValue searchTook;
        private final TimeValue inferenceTook;
        private final SearchResponse response;

        public Response(TimeValue searchTook, TimeValue inferenceTook, SearchResponse response) {
            this.searchTook = searchTook;
            this.inferenceTook = inferenceTook;
            this.response = response;
        }

        public Response(StreamInput in) throws IOException {
            searchTook = in.readTimeValue();
            inferenceTook = in.readTimeValue();
            response = new SearchResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeTimeValue(searchTook);
            out.writeTimeValue(inferenceTook);
            response.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("search_took", searchTook.millis());
            builder.field("inference_took", inferenceTook.millis());
            builder.field("search_response", response);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response1 = (Response) o;
            return Objects.equals(searchTook, response1.searchTook)
                && Objects.equals(inferenceTook, response1.inferenceTook)
                && Objects.equals(response, response1.response);
        }

        @Override
        public int hashCode() {
            return Objects.hash(searchTook, inferenceTook, response);
        }
    }

}
