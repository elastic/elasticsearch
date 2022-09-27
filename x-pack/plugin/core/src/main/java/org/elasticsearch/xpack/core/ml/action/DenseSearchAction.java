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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class DenseSearchAction extends ActionType<DenseSearchAction.Response> {

    public static final String NAME = "cluster:monitor/xpack/ml/dense_search";

    public static final DenseSearchAction INSTANCE = new DenseSearchAction(NAME);

    private DenseSearchAction(String name) {
        super(name, DenseSearchAction.Response::new);
    }

    public static class Request extends ActionRequest {

        public static final ParseField QUERY_STRING = new ParseField("query_string");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField KNN = new ParseField("knn");

        static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME);

        static {
            PARSER.declareString(Request.Builder::setDeploymentId, InferTrainedModelDeploymentAction.Request.DEPLOYMENT_ID);
            PARSER.declareString(Request.Builder::setQueryString, QUERY_STRING);
            PARSER.declareString(Request.Builder::setTimeout, TIMEOUT);
            PARSER.declareObject(
                Request.Builder::setUpdate,
                (p, c) -> TextEmbeddingConfigUpdate.fromXContentStrict(p),
                InferTrainedModelDeploymentAction.Request.INFERENCE_CONFIG
            );
            PARSER.declareObject(
                Request.Builder::setKnnSearch,
                (p, c) -> KnnSearchBuilder.fromXContentWithoutVectorField(p),
                SearchSourceBuilder.KNN_FIELD
            );
            PARSER.declareFieldArray(
                Request.Builder::setFilters,
                (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
                KnnSearchBuilder.FILTER_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY
            );
            PARSER.declareField(
                (p, request, c) -> request.setFetchSource(FetchSourceContext.fromXContent(p)),
                SearchSourceBuilder._SOURCE_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING
            );
            PARSER.declareFieldArray(
                Request.Builder::setFields,
                (p, c) -> FieldAndFormat.fromXContent(p),
                SearchSourceBuilder.FETCH_FIELDS_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY
            );
            PARSER.declareFieldArray(
                Request.Builder::setDocValueFields,
                (p, c) -> FieldAndFormat.fromXContent(p),
                SearchSourceBuilder.DOCVALUE_FIELDS_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY
            );
            PARSER.declareField(
                Request.Builder::setStoredFields,
                (p, c) -> StoredFieldsContext.fromXContent(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), p),
                SearchSourceBuilder.STORED_FIELDS_FIELD,
                ObjectParser.ValueType.STRING_ARRAY
            );
        }

        public static Request parseRestRequest(RestRequest restRequest) throws IOException {
            Builder builder = new Builder(Strings.splitStringByCommaToArray(restRequest.param("index")));
            builder.setRouting(restRequest.param("routing"));
            if (restRequest.hasContentOrSourceParam()) {
                try (XContentParser contentParser = restRequest.contentOrSourceParamParser()) {
                    PARSER.parse(contentParser, builder, null);
                }
            }
            return builder.build();
        }

        private final String[] indices;
        private final String routing;
        private final String queryString;
        private final String deploymentId;
        private final TimeValue inferenceTimeout;
        private final KnnSearchBuilder knnSearchBuilder;
        private final TextEmbeddingConfigUpdate embeddingConfig;
        private final List<QueryBuilder> filters;
        private final FetchSourceContext fetchSource;
        private final List<FieldAndFormat> fields;
        private final List<FieldAndFormat> docValueFields;
        private final StoredFieldsContext storedFields;

        public Request(StreamInput in) throws IOException {
            super(in);
            indices = in.readStringArray();
            routing = in.readOptionalString();
            queryString = in.readString();
            deploymentId = in.readString();
            inferenceTimeout = in.readOptionalTimeValue();
            knnSearchBuilder = new KnnSearchBuilder(in);
            embeddingConfig = in.readOptionalWriteable(TextEmbeddingConfigUpdate::new);
            filters = in.readNamedWriteableList(QueryBuilder.class);
            fetchSource = in.readOptionalWriteable(FetchSourceContext::readFrom);
            fields = in.readOptionalList(FieldAndFormat::new);
            docValueFields = in.readOptionalList(FieldAndFormat::new);
            storedFields = in.readOptionalWriteable(StoredFieldsContext::new);
        }

        Request(
            String[] indices,
            String routing,
            String queryString,
            String deploymentId,
            KnnSearchBuilder knnSearchBuilder,
            TextEmbeddingConfigUpdate embeddingConfig,
            TimeValue inferenceTimeout,
            List<QueryBuilder> filters,
            FetchSourceContext fetchSource,
            List<FieldAndFormat> fields,
            List<FieldAndFormat> docValueFields,
            StoredFieldsContext storedFields
        ) {
            this.indices = indices;
            this.routing = routing;
            this.queryString = queryString;
            this.deploymentId = deploymentId;
            this.knnSearchBuilder = knnSearchBuilder;
            this.embeddingConfig = embeddingConfig;
            this.inferenceTimeout = inferenceTimeout;
            this.filters = filters;
            this.fetchSource = fetchSource;
            this.fields = fields;
            this.docValueFields = docValueFields;
            this.storedFields = storedFields;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            out.writeOptionalString(routing);
            out.writeString(queryString);
            out.writeString(deploymentId);
            out.writeOptionalTimeValue(inferenceTimeout);
            knnSearchBuilder.writeTo(out);
            out.writeOptionalWriteable(embeddingConfig);
            out.writeNamedWriteableList(filters);
            out.writeOptionalWriteable(fetchSource);
            out.writeOptionalCollection(fields);
            out.writeOptionalCollection(docValueFields);
            out.writeOptionalWriteable(storedFields);
        }

        public String[] getIndices() {
            return indices;
        }

        public String getRouting() {
            return routing;
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

        public List<QueryBuilder> getFilters() {
            return filters;
        }

        public FetchSourceContext getFetchSource() {
            return fetchSource;
        }

        public List<FieldAndFormat> getFields() {
            return fields;
        }

        public List<FieldAndFormat> getDocValueFields() {
            return docValueFields;
        }

        public StoredFieldsContext getStoredFields() {
            return storedFields;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(indices, request.indices)
                && Objects.equals(routing, request.routing)
                && Objects.equals(queryString, request.queryString)
                && Objects.equals(deploymentId, request.deploymentId)
                && Objects.equals(inferenceTimeout, request.inferenceTimeout)
                && Objects.equals(knnSearchBuilder, request.knnSearchBuilder)
                && Objects.equals(embeddingConfig, request.embeddingConfig)
                && Objects.equals(filters, request.filters)
                && Objects.equals(fetchSource, request.fetchSource)
                && Objects.equals(fields, request.fields)
                && Objects.equals(docValueFields, request.docValueFields)
                && Objects.equals(storedFields, request.storedFields);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(
                routing,
                queryString,
                deploymentId,
                inferenceTimeout,
                knnSearchBuilder,
                embeddingConfig,
                filters,
                fetchSource,
                fields,
                docValueFields,
                storedFields
            );
            result = 31 * result + Arrays.hashCode(indices);
            return result;
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

            private final String[] indices;
            private String routing;
            private String deploymentId;
            private String queryString;
            private TimeValue timeout;
            private TextEmbeddingConfigUpdate update;
            private KnnSearchBuilder knnSearchBuilder;
            private List<QueryBuilder> filters;
            private FetchSourceContext fetchSource;
            private List<FieldAndFormat> fields;
            private List<FieldAndFormat> docValueFields;
            private StoredFieldsContext storedFields;

            Builder(String[] indices) {
                this.indices = indices;
            }

            void setRouting(String routing) {
                this.routing = routing;
            }

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

            private void setFilters(List<QueryBuilder> filters) {
                this.filters = filters;
            }

            private void setFetchSource(FetchSourceContext fetchSource) {
                this.fetchSource = fetchSource;
            }

            private void setFields(List<FieldAndFormat> fields) {
                this.fields = fields;
            }

            private void setDocValueFields(List<FieldAndFormat> docValueFields) {
                this.docValueFields = docValueFields;
            }

            private void setStoredFields(StoredFieldsContext storedFields) {
                this.storedFields = storedFields;
            }

            Request build() {
                return new Request(
                    indices,
                    routing,
                    queryString,
                    deploymentId,
                    knnSearchBuilder,
                    update,
                    timeout,
                    filters,
                    fetchSource,
                    fields,
                    docValueFields,
                    storedFields
                );
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
