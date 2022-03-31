/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.action;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.vectors.query.KnnVectorQueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A builder used in {@link RestKnnSearchAction} to convert the kNN REST request
 * into a {@link SearchRequestBuilder}.
 */
class KnnSearchRequestBuilder {
    static final String INDEX_PARAM = "index";
    static final String ROUTING_PARAM = "routing";

    static final ParseField KNN_SECTION_FIELD = new ParseField("knn");
    static final ParseField FILTER_FIELD = new ParseField("filter");
    private static final ObjectParser<KnnSearchRequestBuilder, Void> PARSER;

    static {
        PARSER = new ObjectParser<>("knn-search");
        PARSER.declareField(KnnSearchRequestBuilder::knnSearch, KnnSearch::parse, KNN_SECTION_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareFieldArray(
            KnnSearchRequestBuilder::filter,
            (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
            FILTER_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareField(
            (p, request, c) -> request.fetchSource(FetchSourceContext.fromXContent(p)),
            SearchSourceBuilder._SOURCE_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING
        );
        PARSER.declareFieldArray(
            KnnSearchRequestBuilder::fields,
            (p, c) -> FieldAndFormat.fromXContent(p),
            SearchSourceBuilder.FETCH_FIELDS_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareFieldArray(
            KnnSearchRequestBuilder::docValueFields,
            (p, c) -> FieldAndFormat.fromXContent(p),
            SearchSourceBuilder.DOCVALUE_FIELDS_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareField(
            (p, request, c) -> request.storedFields(
                StoredFieldsContext.fromXContent(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), p)
            ),
            SearchSourceBuilder.STORED_FIELDS_FIELD,
            ObjectParser.ValueType.STRING_ARRAY
        );
    }

    /**
     * Parses a {@link RestRequest} representing a kNN search into a request builder.
     */
    static KnnSearchRequestBuilder parseRestRequest(RestRequest restRequest) throws IOException {
        KnnSearchRequestBuilder builder = new KnnSearchRequestBuilder(Strings.splitStringByCommaToArray(restRequest.param("index")));
        builder.routing(restRequest.param("routing"));

        if (restRequest.hasContentOrSourceParam()) {
            try (XContentParser contentParser = restRequest.contentOrSourceParamParser()) {
                PARSER.parse(contentParser, builder, null);
            }
        }
        return builder;
    }

    private final String[] indices;
    private String routing;
    private KnnSearch knnSearch;
    private List<QueryBuilder> filters;

    private FetchSourceContext fetchSource;
    private List<FieldAndFormat> fields;
    private List<FieldAndFormat> docValueFields;
    private StoredFieldsContext storedFields;

    private KnnSearchRequestBuilder(String[] indices) {
        this.indices = indices;
    }

    /**
     * Defines the kNN search to execute.
     */
    private void knnSearch(KnnSearch knnSearch) {
        this.knnSearch = knnSearch;
    }

    private void filter(List<QueryBuilder> filter) {
        this.filters = filter;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    private void routing(String routing) {
        this.routing = routing;
    }

    /**
     * Defines how the _source should be fetched.
     */
    private void fetchSource(FetchSourceContext fetchSource) {
        this.fetchSource = fetchSource;
    }

    /**
     * A list of fields to load and return. The fields must be present in the document _source.
     */
    private void fields(List<FieldAndFormat> fields) {
        this.fields = fields;
    }

    /**
     * A list of docvalue fields to load and return.
     */
    private void docValueFields(List<FieldAndFormat> docValueFields) {
        this.docValueFields = docValueFields;
    }

    /**
     * Defines the stored fields to load and return as part of the search request. To disable the stored
     * fields entirely (source and metadata fields), use {@link StoredFieldsContext#_NONE_}.
     */
    private void storedFields(StoredFieldsContext storedFields) {
        this.storedFields = storedFields;
    }

    /**
     * Adds all the request components to the given {@link SearchRequestBuilder}.
     */
    public void build(SearchRequestBuilder builder) {
        builder.setIndices(indices);
        builder.setRouting(routing);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.trackTotalHitsUpTo(SearchContext.TRACK_TOTAL_HITS_ACCURATE);

        if (knnSearch == null) {
            throw new IllegalArgumentException("missing required [" + KNN_SECTION_FIELD.getPreferredName() + "] section in search body");
        }

        KnnVectorQueryBuilder queryBuilder = knnSearch.buildQuery();
        if (filters != null) {
            queryBuilder.addFilterQueries(this.filters);
        }

        sourceBuilder.query(queryBuilder);
        sourceBuilder.size(knnSearch.k);

        sourceBuilder.fetchSource(fetchSource);
        sourceBuilder.storedFields(storedFields);
        if (fields != null) {
            for (FieldAndFormat field : fields) {
                sourceBuilder.fetchField(field);
            }
        }
        if (docValueFields != null) {
            for (FieldAndFormat field : docValueFields) {
                sourceBuilder.docValueField(field.field, field.format);
            }
        }

        builder.setSource(sourceBuilder);
    }

    // visible for testing
    static class KnnSearch {
        private static final int NUM_CANDS_LIMIT = 10000;
        static final ParseField FIELD_FIELD = new ParseField("field");
        static final ParseField K_FIELD = new ParseField("k");
        static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
        static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");

        private static final ConstructingObjectParser<KnnSearch, Void> PARSER = new ConstructingObjectParser<>("knn", args -> {
            @SuppressWarnings("unchecked")
            List<Float> vector = (List<Float>) args[1];
            float[] vectorArray = new float[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                vectorArray[i] = vector.get(i);
            }
            return new KnnSearch((String) args[0], vectorArray, (int) args[2], (int) args[3]);
        });

        static {
            PARSER.declareString(constructorArg(), FIELD_FIELD);
            PARSER.declareFloatArray(constructorArg(), QUERY_VECTOR_FIELD);
            PARSER.declareInt(constructorArg(), K_FIELD);
            PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
        }

        public static KnnSearch parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        final String field;
        final float[] queryVector;
        final int k;
        final int numCands;

        /**
         * Defines a kNN search.
         *
         * @param field the name of the vector field to search against
         * @param queryVector the query vector
         * @param k the final number of nearest neighbors to return as top hits
         * @param numCands the number of nearest neighbor candidates to consider per shard
         */
        KnnSearch(String field, float[] queryVector, int k, int numCands) {
            this.field = field;
            this.queryVector = queryVector;
            this.k = k;
            this.numCands = numCands;
        }

        public KnnVectorQueryBuilder buildQuery() {
            // We perform validation here instead of the constructor because it makes the errors
            // much clearer. Otherwise, the error message is deeply nested under parsing exceptions.
            if (k < 1) {
                throw new IllegalArgumentException("[" + K_FIELD.getPreferredName() + "] must be greater than 0");
            }
            if (numCands < k) {
                throw new IllegalArgumentException(
                    "[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot be less than " + "[" + K_FIELD.getPreferredName() + "]"
                );
            }
            if (numCands > NUM_CANDS_LIMIT) {
                throw new IllegalArgumentException("[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot exceed [" + NUM_CANDS_LIMIT + "]");
            }

            return new KnnVectorQueryBuilder(field, queryVector, numCands);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KnnSearch that = (KnnSearch) o;
            return k == that.k
                && numCands == that.numCands
                && Objects.equals(field, that.field)
                && Arrays.equals(queryVector, that.queryVector);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(field, k, numCands);
            result = 31 * result + Arrays.hashCode(queryVector);
            return result;
        }
    }
}
