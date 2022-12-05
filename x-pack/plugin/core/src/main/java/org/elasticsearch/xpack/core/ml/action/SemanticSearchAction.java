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
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class SemanticSearchAction extends ActionType<SemanticSearchAction.Response> {

    public static final String NAME = "indices:data/read/semantic_search";

    public static final SemanticSearchAction INSTANCE = new SemanticSearchAction(NAME);

    private SemanticSearchAction(String name) {
        super(name, SemanticSearchAction.Response::new);
    }

    public static class Request extends ActionRequest implements IndicesRequest.Replaceable {

        public static final ParseField QUERY_STRING = new ParseField("query_string"); // TODO a better name and update docs when changed

        static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME);

        static {
            PARSER.declareString(Request.Builder::setModelId, InferModelAction.Request.MODEL_ID);
            PARSER.declareString(Request.Builder::setQueryString, QUERY_STRING);
            PARSER.declareString(Request.Builder::setTimeout, SearchSourceBuilder.TIMEOUT_FIELD);
            PARSER.declareObject(
                Request.Builder::setUpdate,
                (p, c) -> TextEmbeddingConfigUpdate.fromXContentStrict(p),
                InferTrainedModelDeploymentAction.Request.INFERENCE_CONFIG
            );
            PARSER.declareObject(Request.Builder::setKnnSearch, (p, c) -> KnnQueryOptions.fromXContent(p), SearchSourceBuilder.KNN_FIELD);
            PARSER.declareFieldArray(
                Request.Builder::setFilters,
                (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
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

        private String[] indices;
        private final String routing;
        private final String queryString;
        private final String modelId;
        private final TimeValue inferenceTimeout;
        private final KnnQueryOptions knnQueryOptions;
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
            modelId = in.readString();
            inferenceTimeout = in.readOptionalTimeValue();
            knnQueryOptions = new KnnQueryOptions(in);
            embeddingConfig = in.readOptionalWriteable(TextEmbeddingConfigUpdate::new);
            if (in.readBoolean()) {
                filters = in.readNamedWriteableList(QueryBuilder.class);
            } else {
                filters = null;
            }
            fetchSource = in.readOptionalWriteable(FetchSourceContext::readFrom);
            fields = in.readOptionalList(FieldAndFormat::new);
            docValueFields = in.readOptionalList(FieldAndFormat::new);
            storedFields = in.readOptionalWriteable(StoredFieldsContext::new);
        }

        Request(
            String[] indices,
            String routing,
            String queryString,
            String modelId,
            KnnQueryOptions knnQueryOptions,
            TextEmbeddingConfigUpdate embeddingConfig,
            TimeValue inferenceTimeout,
            List<QueryBuilder> filters,
            FetchSourceContext fetchSource,
            List<FieldAndFormat> fields,
            List<FieldAndFormat> docValueFields,
            StoredFieldsContext storedFields
        ) {
            this.indices = Objects.requireNonNull(indices, "[indices] must not be null");
            this.routing = routing;
            this.queryString = queryString;
            this.modelId = modelId;
            this.knnQueryOptions = knnQueryOptions;
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
            out.writeString(modelId);
            out.writeOptionalTimeValue(inferenceTimeout);
            knnQueryOptions.writeTo(out);
            out.writeOptionalWriteable(embeddingConfig);
            if (filters != null) {
                out.writeBoolean(true);
                out.writeNamedWriteableList(filters);
            } else {
                out.writeBoolean(false);
            }
            out.writeOptionalWriteable(fetchSource);
            out.writeOptionalCollection(fields);
            out.writeOptionalCollection(docValueFields);
            out.writeOptionalWriteable(storedFields);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return SearchRequest.DEFAULT_INDICES_OPTIONS;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            Objects.requireNonNull(indices, "indices must not be null");
            for (String index : indices) {
                Objects.requireNonNull(index, "index must not be null");
            }
            this.indices = indices;
            return this;
        }

        public String getRouting() {
            return routing;
        }

        public String getQueryString() {
            return queryString;
        }

        public String getModelId() {
            return modelId;
        }

        public TimeValue getInferenceTimeout() {
            return inferenceTimeout;
        }

        public KnnQueryOptions getKnnQueryOptions() {
            return knnQueryOptions;
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
                && Objects.equals(modelId, request.modelId)
                && Objects.equals(inferenceTimeout, request.inferenceTimeout)
                && Objects.equals(knnQueryOptions, request.knnQueryOptions)
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
                modelId,
                inferenceTimeout,
                knnQueryOptions,
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
            if (modelId == null) {
                error.addValidationError("model_id cannot be null");
            }
            if (knnQueryOptions == null) {
                error.addValidationError("knn cannot be null");
            }

            return error.validationErrors().isEmpty() ? null : error;
        }

        public static class Builder {

            private final String[] indices;
            private String routing;
            private String modelId;
            private String queryString;
            private TimeValue timeout;
            private TextEmbeddingConfigUpdate update;
            private KnnQueryOptions knnSearchBuilder;
            private List<QueryBuilder> filters;
            private FetchSourceContext fetchSource;
            private List<FieldAndFormat> fields;
            private List<FieldAndFormat> docValueFields;
            private StoredFieldsContext storedFields;

            Builder(String[] indices) {
                this.indices = Objects.requireNonNull(indices, "[indices] must not be null");
            }

            void setRouting(String routing) {
                this.routing = routing;
            }

            void setModelId(String modelId) {
                this.modelId = modelId;
            }

            void setQueryString(String queryString) {
                this.queryString = queryString;
            }

            void setTimeout(TimeValue timeout) {
                this.timeout = timeout;
            }

            void setTimeout(String timeout) {
                setTimeout(TimeValue.parseTimeValue(timeout, SearchSourceBuilder.TIMEOUT_FIELD.getPreferredName()));
            }

            void setUpdate(TextEmbeddingConfigUpdate update) {
                this.update = update;
            }

            void setKnnSearch(KnnQueryOptions knnSearchBuilder) {
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
                    modelId,
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
        private final SearchResponse searchResponse;

        public Response(TimeValue searchTook, TimeValue inferenceTook, SearchResponse searchResponse) {
            this.searchTook = searchTook;
            this.inferenceTook = inferenceTook;
            this.searchResponse = searchResponse;
        }

        public Response(StreamInput in) throws IOException {
            searchTook = in.readTimeValue();
            inferenceTook = in.readTimeValue();
            searchResponse = new SearchResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeTimeValue(searchTook);
            out.writeTimeValue(inferenceTook);
            searchResponse.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("inference_took", inferenceTook.millis());
            searchResponse.innerToXContent(builder, params);
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
                && Objects.equals(searchResponse, response1.searchResponse);
        }

        @Override
        public int hashCode() {
            return Objects.hash(searchTook, inferenceTook, searchResponse);
        }
    }

    public static class KnnQueryOptions implements Writeable {

        private static final ConstructingObjectParser<KnnQueryOptions, Void> PARSER = new ConstructingObjectParser<>(
            "knn",
            true,
            args -> new KnnQueryOptions((String) args[0], (int) args[1], (int) args[2])
        );

        static {
            PARSER.declareString(constructorArg(), KnnSearchBuilder.FIELD_FIELD);
            PARSER.declareInt(constructorArg(), KnnSearchBuilder.K_FIELD);
            PARSER.declareInt(constructorArg(), KnnSearchBuilder.NUM_CANDS_FIELD);
            PARSER.declareFieldArray(
                KnnQueryOptions::addFilterQueries,
                (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
                KnnSearchBuilder.FILTER_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY
            );
            PARSER.declareFloat(KnnQueryOptions::boost, KnnSearchBuilder.BOOST_FIELD);
        }

        public static KnnQueryOptions fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        final String field;
        final int k;
        final int numCands;
        final List<QueryBuilder> filterQueries;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        /**
         * Holder for the knn search options.
         * Validation occurs in when this is converted to a {@code KnnSearchBuilder}
         *
         * @param field       the name of the vector field to search against
         * @param k           the final number of nearest neighbors to return as top hits
         * @param numCands    the number of nearest neighbor candidates to consider per shard
         */
        public KnnQueryOptions(String field, int k, int numCands) {
            this.field = field;
            this.k = k;
            this.numCands = numCands;
            this.filterQueries = new ArrayList<>();
        }

        public KnnQueryOptions(StreamInput in) throws IOException {
            this.field = in.readString();
            this.k = in.readVInt();
            this.numCands = in.readVInt();
            this.filterQueries = in.readNamedWriteableList(QueryBuilder.class);
            this.boost = in.readFloat();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(field);
            out.writeVInt(k);
            out.writeVInt(numCands);
            out.writeNamedWriteableList(filterQueries);
            out.writeFloat(boost);
        }

        public void addFilterQueries(List<QueryBuilder> filterQueries) {
            Objects.requireNonNull(filterQueries);
            this.filterQueries.addAll(filterQueries);
        }

        /**
         * Set a boost to apply to the kNN search scores.
         */
        public void boost(float boost) {
            this.boost = boost;
        }

        public KnnSearchBuilder toKnnSearchBuilder(float[] queryVector) {
            if (queryVector == null) {
                throw new IllegalStateException("[query_vector] not set on the Knn query");
            }
            return new KnnSearchBuilder(field, queryVector, k, numCands);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KnnQueryOptions that = (KnnQueryOptions) o;
            return k == that.k
                && numCands == that.numCands
                && Float.compare(that.boost, boost) == 0
                && Objects.equals(field, that.field)
                && Objects.equals(filterQueries, that.filterQueries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, k, numCands, filterQueries, boost);
        }
    }

}
