/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class SemanticMultiMatchQueryRewriteInterceptor extends SemanticQueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_MULTI_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_multi_match_query_rewrite_interception_supported"
    );

    private final Supplier<ModelRegistry> modelRegistrySupplier;

    public SemanticMultiMatchQueryRewriteInterceptor(Supplier<ModelRegistry> modelRegistrySupplier) {
        super();
        this.modelRegistrySupplier = modelRegistrySupplier;
    }

    @Override
    protected String getFieldName(QueryBuilder queryBuilder) {
        MultiMatchQueryBuilder multiMatchQuery = (MultiMatchQueryBuilder) queryBuilder;
        Map<String, Float> fields = multiMatchQuery.fields();
        if (fields.size() > 1) {
            throw new IllegalArgumentException("getFieldName() called on MultiMatchQuery with multiple fields");
        }
        return fields.keySet().iterator().next();
    }

    @Override
    protected Map<String, Float> getFieldsWithWeights(QueryBuilder queryBuilder) {
        MultiMatchQueryBuilder multiMatchQuery = (MultiMatchQueryBuilder) queryBuilder;
        return multiMatchQuery.fields();
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        MultiMatchQueryBuilder multiMatchQuery = (MultiMatchQueryBuilder) queryBuilder;
        return (String) multiMatchQuery.value();
    }

    @Override
    protected QueryBuilder buildInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        MultiMatchQueryBuilder originalQuery = (MultiMatchQueryBuilder) queryBuilder;
        Map<String, Float> fieldsBoosts = getFieldsWithWeights(queryBuilder);
        String queryValue = getQuery(queryBuilder);
        Set<String> inferenceFields = indexInformation.getAllInferenceFields();

        if (inferenceFields.size() == 1) {
            // Single inference field - all multi_match types work the same (like original Elasticsearch)
            // No validation needed since single field queries don't require type-specific combination logic
            String fieldName = inferenceFields.iterator().next();
            SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);

            // Apply per-field boost
            float fieldBoost = fieldsBoosts.getOrDefault(fieldName, AbstractQueryBuilder.DEFAULT_BOOST);

            // Apply top-level query boost with per field and name
            semanticQuery.boost(fieldBoost * originalQuery.boost());
            semanticQuery.queryName(originalQuery.queryName());
            return semanticQuery;
        } else {
            // Multiple inference fields - handle based on multi-match query type (validation happens here)
            return buildMultiFieldSemanticQuery(originalQuery, fieldsBoosts, inferenceFields, queryValue);
        }
    }

    @Override
    protected QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation
    ) {
        MultiMatchQueryBuilder originalQuery = (MultiMatchQueryBuilder) queryBuilder;
        Map<String, Float> fieldsBoosts = getFieldsWithWeights(queryBuilder);
        String queryValue = getQuery(queryBuilder);

        validateQueryTypeSupported(originalQuery.type());

        return switch (originalQuery.type()) {
            case BEST_FIELDS -> buildBestFieldsCombinedQuery(originalQuery, fieldsBoosts, indexInformation, queryValue);
            case MOST_FIELDS -> buildMostFieldsCombinedQuery(originalQuery, fieldsBoosts, indexInformation, queryValue);
            default ->
                // Fallback to best_fields behavior
                buildBestFieldsCombinedQuery(originalQuery, fieldsBoosts, indexInformation, queryValue);
        };
    }

    @Override
    public String getQueryName() {
        return MultiMatchQueryBuilder.NAME;
    }

    private QueryBuilder buildMultiFieldSemanticQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        Set<String> inferenceFields,
        String queryValue
    ) {
        return switch (originalQuery.type()) {
            case BEST_FIELDS -> buildBestFieldsSemanticQuery(originalQuery, fieldsBoosts, inferenceFields, queryValue);
            case MOST_FIELDS -> buildMostFieldsSemanticQuery(originalQuery, fieldsBoosts, inferenceFields, queryValue);
            default ->
                // Fallback to best_fields behavior for unknown types
                buildBestFieldsSemanticQuery(originalQuery, fieldsBoosts, inferenceFields, queryValue);
        };
    }


    /**
     * Validates that the multi_match query type is supported for semantic_text fields.
     * Throws IllegalArgumentException for unsupported types.
     */
    private void validateQueryTypeSupported(MultiMatchQueryBuilder.Type queryType) {
        switch (queryType) {
            case CROSS_FIELDS:
                throw new IllegalArgumentException(
                    "multi_match query with type [cross_fields] is not supported for semantic_text fields. "
                        + "Use [best_fields] or [most_fields] instead."
                );
            case PHRASE:
                throw new IllegalArgumentException(
                    "multi_match query with type [phrase] is not supported for semantic_text fields. " + "Use [best_fields] instead."
                );
            case PHRASE_PREFIX:
                throw new IllegalArgumentException(
                    "multi_match query with type [phrase_prefix] is not supported for semantic_text fields. " + "Use [best_fields] instead."
                );
            case BOOL_PREFIX:
                throw new IllegalArgumentException(
                    "multi_match query with type [bool_prefix] is not supported for semantic_text fields. "
                        + "Use [best_fields] or [most_fields] instead."
                );
        }
    }

    /**
     * Creates a semantic query with field boost applied.
     */
    private SemanticQueryBuilder createSemanticQuery(
        String fieldName,
        String queryValue,
        Map<String, Float> fieldsBoosts,
        boolean lenient
    ) {
        SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, lenient);
        float fieldBoost = fieldsBoosts.getOrDefault(fieldName, AbstractQueryBuilder.DEFAULT_BOOST);
        semanticQuery.boost(fieldBoost);
        return semanticQuery;
    }


    private QueryBuilder  buildBestFieldsSemanticQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        Set<String> inferenceFields,
        String queryValue
    ) {
        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();
        for (String fieldName : inferenceFields) {
            disMaxQuery.add(createSemanticQuery(fieldName, queryValue, fieldsBoosts, false));
        }
        // Apply tie_breaker - use explicit value or fall back to type's default
        Float tieBreaker = originalQuery.tieBreaker();
        if (tieBreaker != null) {
            disMaxQuery.tieBreaker(tieBreaker);
        } else {
            disMaxQuery.tieBreaker(originalQuery.type().tieBreaker());
        }
        disMaxQuery.boost(originalQuery.boost());
        disMaxQuery.queryName(originalQuery.queryName());
        return disMaxQuery;
    }

    /**
     * Builds a most_fields query for pure semantic fields using BoolQueryBuilder.
     */
    private QueryBuilder buildMostFieldsSemanticQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        Set<String> inferenceFields,
        String queryValue
    ) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (String fieldName : inferenceFields) {
            boolQuery.should(createSemanticQuery(fieldName, queryValue, fieldsBoosts, false));
        }
        // Apply minimumShouldMatch - use original query's value or default to "1"
        String minimumShouldMatch = originalQuery.minimumShouldMatch();
        boolQuery.minimumShouldMatch(minimumShouldMatch != null ? minimumShouldMatch : "1");
        boolQuery.boost(originalQuery.boost());
        boolQuery.queryName(originalQuery.queryName());
        return boolQuery;
    }

    private QueryBuilder buildBestFieldsCombinedQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        InferenceIndexInformationForField inferenceInfo,
        String queryValue
    ) {
        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();

        // Add semantic queries for each inference field across different indices
        for (String fieldName : inferenceInfo.getAllInferenceFields()) {
            disMaxQuery.add(
                createSemanticSubQuery(
                    inferenceInfo.getInferenceIndices(),
                    fieldName,
                    queryValue
                ).boost(fieldsBoosts.getOrDefault(fieldName, AbstractQueryBuilder.DEFAULT_BOOST))
            );
        }

        // Add non-inference query for non-inference fields
        if (inferenceInfo.hasNonInferenceFields()) {
            MultiMatchQueryBuilder nonInferenceQuery = createNonInferenceQueryForIndex(
                originalQuery,
                inferenceInfo.getAllNonInferenceFields(),
                fieldsBoosts
            );
            disMaxQuery.add(createSubQueryForIndices(inferenceInfo.nonInferenceIndices(), nonInferenceQuery));
        }

        // Apply tie_breaker - use explicit value or fall back to type's default
        Float tieBreaker = originalQuery.tieBreaker();
        if (tieBreaker != null) {
            disMaxQuery.tieBreaker(tieBreaker);
        } else {
            disMaxQuery.tieBreaker(originalQuery.type().tieBreaker());
        }
        disMaxQuery.boost(originalQuery.boost());
        disMaxQuery.queryName(originalQuery.queryName());
        return disMaxQuery;
    }

    private QueryBuilder buildMostFieldsCombinedQuery(
        MultiMatchQueryBuilder originalQuery,
        Map<String, Float> fieldsBoosts,
        InferenceIndexInformationForField inferenceInfo,
        String queryValue
    ) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        // Add semantic queries for each inference field
        for (String fieldName : inferenceInfo.getAllInferenceFields()) {
            boolQuery.should(
                createSemanticSubQuery(
                    inferenceInfo.getInferenceIndices(),
                    fieldName,
                    queryValue
                ).boost(fieldsBoosts.getOrDefault(fieldName, AbstractQueryBuilder.DEFAULT_BOOST))
            );
        }

        // Add non-inference query for non-inference fields
        if (inferenceInfo.hasNonInferenceFields()) {
            MultiMatchQueryBuilder nonInferenceQuery = createNonInferenceQueryForIndex(
                originalQuery,
                inferenceInfo.getAllNonInferenceFields(),
                fieldsBoosts
            );
            boolQuery.should(createSubQueryForIndices(inferenceInfo.nonInferenceIndices(), nonInferenceQuery));
        }

        // Apply minimumShouldMatch - use original query's value or default to "1"
        String minimumShouldMatch = originalQuery.minimumShouldMatch();
        boolQuery.minimumShouldMatch(minimumShouldMatch != null ? minimumShouldMatch : "1");
        boolQuery.boost(originalQuery.boost());
        boolQuery.queryName(originalQuery.queryName());
        return boolQuery;
    }

    /**
     * Copies all properties from original query to target query except fields.
     */
    private void copyQueryProperties(MultiMatchQueryBuilder original, MultiMatchQueryBuilder target) {
        target.type(original.type());
        target.operator(original.operator());
        target.slop(original.slop());
        target.analyzer(original.analyzer());
        target.minimumShouldMatch(original.minimumShouldMatch());
        target.fuzzyRewrite(original.fuzzyRewrite());
        target.prefixLength(original.prefixLength());
        target.maxExpansions(original.maxExpansions());
        target.fuzzyTranspositions(original.fuzzyTranspositions());
        target.lenient(original.lenient());
        target.zeroTermsQuery(original.zeroTermsQuery());
        target.autoGenerateSynonymsPhraseQuery(original.autoGenerateSynonymsPhraseQuery());
        target.tieBreaker(original.tieBreaker());

        if (original.fuzziness() != null) {
            target.fuzziness(original.fuzziness());
        }
    }

    /**
     * Creates a non-inference MultiMatchQuery for a specific index with only the specified fields.
     */
    private MultiMatchQueryBuilder createNonInferenceQueryForIndex(
        MultiMatchQueryBuilder originalQuery,
        Set<String> nonInferenceFields,
        Map<String, Float> fieldsBoosts
    ) {
        MultiMatchQueryBuilder query = new MultiMatchQueryBuilder(originalQuery.value());

        // Set only the non-inference fields with their boosts
        Map<String, Float> filteredFields = new HashMap<>();
        for (String fieldName : nonInferenceFields) {
            float boost = fieldsBoosts.getOrDefault(fieldName, AbstractQueryBuilder.DEFAULT_BOOST);
            filteredFields.put(fieldName, boost);
        }
        query.fields(filteredFields);

        copyQueryProperties(originalQuery, query);

        return query;
    }

}
