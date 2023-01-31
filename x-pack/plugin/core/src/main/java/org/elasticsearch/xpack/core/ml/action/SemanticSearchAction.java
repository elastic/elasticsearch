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

        public static final ParseField MODEL_TEXT = new ParseField("model_text"); // TODO a better name and update docs when changed
        public static final ParseField TEXT_EMBEDDING_CONFIG = new ParseField("text_embedding_config");

        static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME);

        static {
            PARSER.declareString(Request.Builder::setModelId, InferModelAction.Request.MODEL_ID);
            PARSER.declareString(Request.Builder::setModelText, MODEL_TEXT);
            PARSER.declareString(Request.Builder::setTimeout, SearchSourceBuilder.TIMEOUT_FIELD);
            PARSER.declareObject(
                Request.Builder::setUpdate,
                (p, c) -> TextEmbeddingConfigUpdate.fromXContentStrict(p),
                TEXT_EMBEDDING_CONFIG
            );
            PARSER.declareObject(
                Request.Builder::setQueryBuilder,
                (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
                SearchSourceBuilder.QUERY_FIELD
            );
            PARSER.declareObject(Request.Builder::setKnnSearch, (p, c) -> KnnQueryOptions.fromXContent(p), SearchSourceBuilder.KNN_FIELD);
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
            PARSER.declareInt(Request.Builder::setSize, SearchSourceBuilder.SIZE_FIELD);
        }

        public static Request parseRestRequest(RestRequest restRequest) throws IOException {
            Builder builder = new Builder(Strings.splitStringByCommaToArray(restRequest.param("index")));
            if (restRequest.hasContentOrSourceParam()) {
                try (XContentParser contentParser = restRequest.contentOrSourceParamParser()) {
                    PARSER.parse(contentParser, builder, null);
                }
            }
            // Query parameters are preferred to body parameters.
            if (restRequest.hasParam("size")) {
                builder.setSize(restRequest.paramAsInt("size", -1));
            }
            builder.setRouting(restRequest.param("routing"));
            return builder.build();
        }

        private String[] indices;
        private final String routing;
        private final String modelText;
        private final String modelId;
        private final TimeValue inferenceTimeout;
        private final QueryBuilder query;
        private final KnnQueryOptions knnQueryOptions;
        private final TextEmbeddingConfigUpdate embeddingConfig;
        private final FetchSourceContext fetchSource;
        private final List<FieldAndFormat> fields;
        private final List<FieldAndFormat> docValueFields;
        private final StoredFieldsContext storedFields;
        private final int size;

        public Request(StreamInput in) throws IOException {
            super(in);
            indices = in.readStringArray();
            routing = in.readOptionalString();
            modelText = in.readString();
            modelId = in.readString();
            inferenceTimeout = in.readOptionalTimeValue();
            query = in.readOptionalNamedWriteable(QueryBuilder.class);
            knnQueryOptions = new KnnQueryOptions(in);
            embeddingConfig = in.readOptionalWriteable(TextEmbeddingConfigUpdate::new);
            fetchSource = in.readOptionalWriteable(FetchSourceContext::readFrom);
            fields = in.readOptionalList(FieldAndFormat::new);
            docValueFields = in.readOptionalList(FieldAndFormat::new);
            storedFields = in.readOptionalWriteable(StoredFieldsContext::new);
            size = in.readInt();
        }

        Request(
            String[] indices,
            String routing,
            String modelText,
            String modelId,
            QueryBuilder query,
            KnnQueryOptions knnQueryOptions,
            TextEmbeddingConfigUpdate embeddingConfig,
            TimeValue inferenceTimeout,
            FetchSourceContext fetchSource,
            List<FieldAndFormat> fields,
            List<FieldAndFormat> docValueFields,
            StoredFieldsContext storedFields,
            int size
        ) {
            this.indices = Objects.requireNonNull(indices, "[indices] must not be null");
            this.routing = routing;
            this.modelText = modelText;
            this.modelId = modelId;
            this.query = query;
            this.knnQueryOptions = knnQueryOptions;
            this.embeddingConfig = embeddingConfig;
            this.inferenceTimeout = inferenceTimeout;
            this.fetchSource = fetchSource;
            this.fields = fields;
            this.docValueFields = docValueFields;
            this.storedFields = storedFields;
            this.size = size;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            out.writeOptionalString(routing);
            out.writeString(modelText);
            out.writeString(modelId);
            out.writeOptionalTimeValue(inferenceTimeout);
            out.writeOptionalNamedWriteable(query);
            knnQueryOptions.writeTo(out);
            out.writeOptionalWriteable(embeddingConfig);
            out.writeOptionalWriteable(fetchSource);
            out.writeOptionalCollection(fields);
            out.writeOptionalCollection(docValueFields);
            out.writeOptionalWriteable(storedFields);
            out.writeInt(size);
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

        public String getModelText() {
            return modelText;
        }

        public String getModelId() {
            return modelId;
        }

        public TimeValue getInferenceTimeout() {
            return inferenceTimeout;
        }

        public QueryBuilder getQuery() {
            return query;
        }

        public KnnQueryOptions getKnnQueryOptions() {
            return knnQueryOptions;
        }

        public TextEmbeddingConfigUpdate getEmbeddingConfig() {
            return embeddingConfig;
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

        public int getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(indices, request.indices)
                && Objects.equals(routing, request.routing)
                && Objects.equals(modelText, request.modelText)
                && Objects.equals(modelId, request.modelId)
                && Objects.equals(inferenceTimeout, request.inferenceTimeout)
                && Objects.equals(query, request.query)
                && Objects.equals(knnQueryOptions, request.knnQueryOptions)
                && Objects.equals(embeddingConfig, request.embeddingConfig)
                && Objects.equals(fetchSource, request.fetchSource)
                && Objects.equals(fields, request.fields)
                && Objects.equals(docValueFields, request.docValueFields)
                && Objects.equals(storedFields, request.storedFields)
                && size == request.size;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(
                routing,
                modelText,
                modelId,
                inferenceTimeout,
                query,
                knnQueryOptions,
                embeddingConfig,
                fetchSource,
                fields,
                docValueFields,
                storedFields,
                size
            );
            result = 31 * result + Arrays.hashCode(indices);
            return result;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException error = new ActionRequestValidationException();
            if (modelText == null) {
                error.addValidationError("model_text cannot be null");
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
            private String modelText;
            private TimeValue timeout;
            private TextEmbeddingConfigUpdate update;
            private QueryBuilder queryBuilder;
            private KnnQueryOptions knnSearchBuilder;
            private FetchSourceContext fetchSource;
            private List<FieldAndFormat> fields;
            private List<FieldAndFormat> docValueFields;
            private StoredFieldsContext storedFields;
            private int size = -1;

            Builder(String[] indices) {
                this.indices = Objects.requireNonNull(indices, "[indices] must not be null");
            }

            void setRouting(String routing) {
                this.routing = routing;
            }

            void setModelId(String modelId) {
                this.modelId = modelId;
            }

            void setModelText(String modelText) {
                this.modelText = modelText;
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

            void setQueryBuilder(QueryBuilder queryBuilder) {
                this.queryBuilder = queryBuilder;
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

            private void setSize(int size) {
                this.size = size;
            }

            Request build() {
                return new Request(
                    indices,
                    routing,
                    modelText,
                    modelId,
                    queryBuilder,
                    knnSearchBuilder,
                    update,
                    timeout,
                    fetchSource,
                    fields,
                    docValueFields,
                    storedFields,
                    size
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
            var builder = new KnnSearchBuilder(field, queryVector, k, numCands);
            builder.boost(boost);
            if (filterQueries.isEmpty() == false) {
                builder.addFilterQueries(filterQueries);
            }
            return builder;
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
