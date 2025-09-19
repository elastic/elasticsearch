/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ResolvedIndices;
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
        if (queryRewriteContext.getMinTransportVersion().before(TransportVersions.MULTI_MATCH_WITH_NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
            return originalQuery;
        }

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
        // No inference fields - return original query
        if (inferenceFields.isEmpty()) {
            return originalQuery;
        }

        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();

        // Add semantic queries for inference fields
        for (Map.Entry<String, Float> field : inferenceFields.entrySet()) {
            disMaxQuery.add(createSemanticQuery(field.getKey(), getQuery(), field.getValue()));
        }

        // Add lexical multi_match query for non-inference fields
        if (nonInferenceFields.isEmpty() == false) {
            disMaxQuery.add(createNonInferenceQuery(nonInferenceFields, getQuery()));
        }

        // Apply tie_breaker - use explicit value or fall back to type's default
        Float tieBreaker = originalQuery.tieBreaker();
        disMaxQuery.tieBreaker(Objects.requireNonNullElseGet(tieBreaker, () -> originalQuery.type().tieBreaker()));

        // Apply query-level boost and name
        return disMaxQuery.boost(originalQuery.boost()).queryName(originalQuery.queryName());
    }

    @Override
    protected boolean resolveWildcards() {
        return false;
    }

    @Override
    protected boolean useDefaultFields() {
        return true;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void coordinatorNodeValidate(ResolvedIndices resolvedIndices) {
        validateQueryTypeSupported(originalQuery.type());
    }

    private SemanticQueryBuilder createSemanticQuery(String fieldName, String queryValue, Float fieldBoost) {
        SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, queryValue, null, inferenceResultsMap);

        // Apply field-level boost
        if (fieldBoost != null && fieldBoost != 1.0f) {
            semanticQuery.boost(fieldBoost);
        }

        return semanticQuery;
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
            case CROSS_FIELDS, PHRASE, PHRASE_PREFIX, BOOL_PREFIX:
                String supportedTypes = (queryType == MultiMatchQueryBuilder.Type.PHRASE
                    || queryType == MultiMatchQueryBuilder.Type.PHRASE_PREFIX) ? "[best_fields]" : "[best_fields] or [most_fields]";
                throw new IllegalArgumentException(
                    "multi_match query with type ["
                        + queryType.toString().toLowerCase()
                        + "] is not supported for semantic_text fields. "
                        + "Use "
                        + supportedTypes
                        + " instead."
                );
        }
    }
}
