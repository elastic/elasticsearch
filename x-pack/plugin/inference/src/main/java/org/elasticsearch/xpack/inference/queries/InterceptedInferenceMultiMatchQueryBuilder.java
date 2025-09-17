/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.InferenceResults;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class InterceptedInferenceMultiMatchQueryBuilder extends InterceptedInferenceQueryBuilder<MultiMatchQueryBuilder> {
    public static final String NAME = "intercepted_inference_multi_match";
    public static final NodeFeature SEMANTIC_TEXT_SUPPORTS_MULTI_MATCH_QUERY = new NodeFeature(
        "search.semantic_text_supports_multi_match_query"
    );

    public InterceptedInferenceMultiMatchQueryBuilder(MultiMatchQueryBuilder originalQuery) {
        super(originalQuery);
    }

    public InterceptedInferenceMultiMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    private InterceptedInferenceMultiMatchQueryBuilder(
        InterceptedInferenceQueryBuilder<MultiMatchQueryBuilder> other,
        Map<String, InferenceResults> inferenceResultsMap
    ) {
        super(other, inferenceResultsMap);
    }

    @Override
    protected Map<String, Float> getFields() {
        return originalQuery.fields();
    }

    @Override
    protected String getQuery() {
        return (String) originalQuery.value();
    }

    @Override
    protected QueryBuilder doRewriteBwC(QueryRewriteContext queryRewriteContext) {
        return this;
    }

    @Override
    protected QueryBuilder copy(Map<String, InferenceResults> inferenceResultsMap) {
        return new InterceptedInferenceMultiMatchQueryBuilder(this, inferenceResultsMap);
    }

    @Override
    protected QueryBuilder queryFields(
        Map<String, Float> inferenceFields,
        Map<String, Float> nonInferenceFields,
        QueryRewriteContext indexMetadataContext
    ) {
        validateQueryTypeSupported(originalQuery.type());

        // No inference fields are present
        if (inferenceFields.isEmpty()) {
            return originalQuery;
        }

        // Only semantic field(s)
        if (nonInferenceFields.isEmpty()) {
            return buildSemanticQuery(inferenceFields, getQuery());
        }

        // Both semantic and non-semantic fields
        return buildCombinedQuery(inferenceFields, nonInferenceFields, getQuery());
    }

    @Override
    protected boolean resolveWildcards() {
        return false;
    }

    @Override
    protected boolean useDefaultFields() {
        return false;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private QueryBuilder buildSemanticQuery(Map<String, Float> inferenceFields, String queryValue) {
        // Single field
        if (inferenceFields.size() == 1) {
            Map.Entry<String, Float> field = inferenceFields.entrySet().iterator().next();
            SemanticQueryBuilder semanticQuery = createSemanticQuery(field.getKey(), queryValue, field.getValue());
            return semanticQuery.boost(semanticQuery.boost() * originalQuery.boost()).queryName(originalQuery.queryName());
        }

        // Multiple fields
        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();
        for (Map.Entry<String, Float> field : inferenceFields.entrySet()) {
            disMaxQuery.add(createSemanticQuery(field.getKey(), queryValue, field.getValue()));
        }

        // Apply tie_breaker - use explicit value or fall back to type's default
        Float tieBreaker = originalQuery.tieBreaker();
        disMaxQuery.tieBreaker(Objects.requireNonNullElseGet(tieBreaker, () -> originalQuery.type().tieBreaker()));

        // Apply query-level boost and name
        return disMaxQuery.boost(originalQuery.boost()).queryName(originalQuery.queryName());
    }

    private SemanticQueryBuilder createSemanticQuery(String fieldName, String queryValue, Float fieldBoost) {
        SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, null, inferenceResultsMap);

        // Apply field-level boost
        if (fieldBoost != null && fieldBoost != 1.0f) {
            semanticQuery.boost(fieldBoost);
        }

        return semanticQuery;
    }

    private QueryBuilder buildCombinedQuery(Map<String, Float> inferenceFields, Map<String, Float> nonInferenceFields, String queryValue) {
        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();

        // Add semantic queries for inference fields
        for (Map.Entry<String, Float> field : inferenceFields.entrySet()) {
            disMaxQuery.add(createSemanticQuery(field.getKey(), queryValue, field.getValue()));
        }

        // Add traditional multi_match query for non-inference fields
        if (nonInferenceFields.isEmpty() == false) {
            MultiMatchQueryBuilder nonInferenceQuery = createNonInferenceQuery(nonInferenceFields, queryValue);
            disMaxQuery.add(nonInferenceQuery);
        }

        // Apply tie_breaker - use explicit value or fall back to type's default
        Float tieBreaker = originalQuery.tieBreaker();
        disMaxQuery.tieBreaker(Objects.requireNonNullElseGet(tieBreaker, () -> originalQuery.type().tieBreaker()));

        // Apply query-level boost and name
        return disMaxQuery.boost(originalQuery.boost()).queryName(originalQuery.queryName());
    }

    private MultiMatchQueryBuilder createNonInferenceQuery(Map<String, Float> nonInferenceFields, String queryValue) {
        MultiMatchQueryBuilder nonInferenceQuery = new MultiMatchQueryBuilder(queryValue);
        nonInferenceQuery.fields(nonInferenceFields);

        // Copy relevant properties from an original query (excluding boost and name which are applied at DisMax level)
        nonInferenceQuery.type(originalQuery.type());
        nonInferenceQuery.operator(originalQuery.operator());
        if (originalQuery.analyzer() != null) {
            nonInferenceQuery.analyzer(originalQuery.analyzer());
        }
        if (originalQuery.fuzziness() != null) {
            nonInferenceQuery.fuzziness(originalQuery.fuzziness());
        }
        nonInferenceQuery.prefixLength(originalQuery.prefixLength());
        nonInferenceQuery.maxExpansions(originalQuery.maxExpansions());
        if (originalQuery.minimumShouldMatch() != null) {
            nonInferenceQuery.minimumShouldMatch(originalQuery.minimumShouldMatch());
        }
        nonInferenceQuery.slop(originalQuery.slop());
        if (originalQuery.tieBreaker() != null) {
            nonInferenceQuery.tieBreaker(originalQuery.tieBreaker());
        }
        nonInferenceQuery.zeroTermsQuery(originalQuery.zeroTermsQuery());
        nonInferenceQuery.autoGenerateSynonymsPhraseQuery(originalQuery.autoGenerateSynonymsPhraseQuery());
        nonInferenceQuery.fuzzyTranspositions(originalQuery.fuzzyTranspositions());
        nonInferenceQuery.lenient(originalQuery.lenient());
        if (originalQuery.fuzzyRewrite() != null) {
            nonInferenceQuery.fuzzyRewrite(originalQuery.fuzzyRewrite());
        }

        return nonInferenceQuery;
    }

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
}
