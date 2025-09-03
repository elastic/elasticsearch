/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SemanticMultiMatchQueryRewriteInterceptor extends SemanticQueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_MULTI_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_multi_match_query_rewrite_interception_supported"
    );

    public SemanticMultiMatchQueryRewriteInterceptor() {}

    @Override
    protected Map<String, Float> getFieldNamesWithBoosts(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQuery = (MultiMatchQueryBuilder) queryBuilder;
        return multiMatchQuery.fields();
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQuery = (MultiMatchQueryBuilder) queryBuilder;
        return (String) multiMatchQuery.value();
    }

    @Override
    protected QueryBuilder buildInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder originalQuery = (MultiMatchQueryBuilder) queryBuilder;
        String queryValue = getQuery(queryBuilder);

        validateQueryTypeSupported(originalQuery.type());
        Set<String> inferenceFields = indexInformation.getAllInferenceFields();

        if (inferenceFields.size() == 1) {
            String fieldName = inferenceFields.iterator().next();
            SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);

            // Apply top-level query boost with per field and name
            semanticQuery.boost(indexInformation.getFieldBoost(fieldName) * originalQuery.boost());
            semanticQuery.queryName(originalQuery.queryName());
            return semanticQuery;
        } else {
            return buildMultiFieldSemanticQuery(originalQuery, inferenceFields, queryValue, indexInformation);
        }
    }

    @Override
    protected QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation
    ) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder originalQuery = (MultiMatchQueryBuilder) queryBuilder;
        String queryValue = getQuery(queryBuilder);

        validateQueryTypeSupported(originalQuery.type());

        return switch (originalQuery.type()) {
            case BEST_FIELDS, MOST_FIELDS -> buildCombinedQuery(originalQuery, indexInformation, queryValue);
            default -> throw new IllegalArgumentException("Unsupported query type [" + originalQuery.type() + "] for semantic_text fields");
        };
    }

    @Override
    public String getQueryName() {
        return MultiMatchQueryBuilder.NAME;
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

    private QueryBuilder buildMultiFieldSemanticQuery(
        MultiMatchQueryBuilder originalQuery,
        Set<String> inferenceFields,
        String queryValue,
        InferenceIndexInformationForField indexInformation
    ) {
        return switch (originalQuery.type()) {
            case BEST_FIELDS, MOST_FIELDS -> buildSemanticQuery(originalQuery, indexInformation, inferenceFields, queryValue);
            default -> throw new IllegalArgumentException("Unsupported query type [" + originalQuery.type() + "] for semantic_text fields");
        };
    }

    private QueryBuilder buildSemanticQuery(
        MultiMatchQueryBuilder originalQuery,
        InferenceIndexInformationForField indexInformation,
        Set<String> inferenceFields,
        String queryValue
    ) {
        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();
        for (String fieldName : inferenceFields) {
            disMaxQuery.add(createSemanticQuery(fieldName, queryValue, indexInformation));
        }

        // Apply tie_breaker - use explicit value or fall back to type's default
        Float tieBreaker = originalQuery.tieBreaker();
        disMaxQuery.tieBreaker(Objects.requireNonNullElseGet(tieBreaker, () -> originalQuery.type().tieBreaker()));
        disMaxQuery.boost(originalQuery.boost());
        disMaxQuery.queryName(originalQuery.queryName());
        return disMaxQuery;
    }

    private SemanticQueryBuilder createSemanticQuery(String fieldName, String queryValue, InferenceIndexInformationForField inferenceInfo) {
        SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, false);
        semanticQuery.boost(inferenceInfo.getFieldBoost(fieldName));
        return semanticQuery;
    }

    private QueryBuilder buildCombinedQuery(
        MultiMatchQueryBuilder originalQuery,
        InferenceIndexInformationForField inferenceInfo,
        String queryValue
    ) {
        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();

        // Add semantic queries
        for (String fieldName : inferenceInfo.getAllInferenceFields()) {
            Set<String> semanticIndices = inferenceInfo.getInferenceIndicesForField(fieldName);
            if (semanticIndices.isEmpty() == false) {
                disMaxQuery.add(
                    createSemanticSubQuery(semanticIndices, fieldName, queryValue).boost(inferenceInfo.getFieldBoost(fieldName))
                );
            }
        }

        // Add non-inference queries
        addNonInferenceQueries(disMaxQuery::add, originalQuery, inferenceInfo);

        // Apply tie_breaker - use explicit value or fall back to type's default
        Float tieBreaker = originalQuery.tieBreaker();
        disMaxQuery.tieBreaker(Objects.requireNonNullElseGet(tieBreaker, () -> originalQuery.type().tieBreaker()));
        disMaxQuery.boost(originalQuery.boost());
        disMaxQuery.queryName(originalQuery.queryName());
        return disMaxQuery;
    }

    private void addNonInferenceQueries(
        java.util.function.Consumer<QueryBuilder> addQuery,
        MultiMatchQueryBuilder originalQuery,
        InferenceIndexInformationForField inferenceInfo
    ) {
        for (Map.Entry<String, Set<String>> entry : inferenceInfo.nonInferenceFieldsPerIndex().entrySet()) {
            String indexName = entry.getKey();
            Set<String> indexFieldNames = entry.getValue();

            Map<String, Float> indexFields = new HashMap<>();
            for (String fieldName : indexFieldNames) {
                indexFields.put(fieldName, inferenceInfo.getFieldBoost(fieldName));
            }

            MultiMatchQueryBuilder indexQuery = new MultiMatchQueryBuilder(originalQuery.value());
            indexQuery.fields(indexFields);
            copyQueryProperties(originalQuery, indexQuery);

            addQuery.accept(createSubQueryForIndices(List.of(indexName), indexQuery));
        }
    }

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
}
