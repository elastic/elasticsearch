/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

public class SemanticMultiMatchQueryRewriteInterceptor extends SemanticQueryRewriteInterceptor {
    @Override
    protected Map<String, Float> getFieldNamesWithWeights(QueryBuilder queryBuilder) {
        assert  (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQueryBuilder = (MultiMatchQueryBuilder) queryBuilder;
        return multiMatchQueryBuilder.fields();
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQueryBuilder = (MultiMatchQueryBuilder) queryBuilder;
        return (String) multiMatchQueryBuilder.value();
    }

    @Override
    protected QueryBuilder buildInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        SemanticQueryBuilder semanticQueryBuilder = new SemanticQueryBuilder(
            indexInformation.fieldName(),
            getQuery(queryBuilder),
            false
        );
        // TODO:: add boost
        semanticQueryBuilder.queryName(queryBuilder.queryName());
        return semanticQueryBuilder;
    }

    @Override
    protected QueryBuilder buildCombinedInferenceAndNonInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder originalMultiMatchQueryBuilder = (MultiMatchQueryBuilder) queryBuilder;

        // Create a copy for non-inference fields with only this specific field
        MultiMatchQueryBuilder multiMatchQueryBuilder = createSingleFieldMultiMatch(
            originalMultiMatchQueryBuilder,
            indexInformation.fieldName()
        );

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // Add semantic query for inference indices
        boolQueryBuilder.should(
            createSemanticSubQuery(
                indexInformation.getInferenceIndices(),
                indexInformation.fieldName(),
                getQuery(queryBuilder)
            )
        );

        // Add regular query for non-inference indices
        boolQueryBuilder.should(
            createSubQueryForIndices(indexInformation.nonInferenceIndices(), multiMatchQueryBuilder)
        );

        // TODO:: add boost
        boolQueryBuilder.queryName(queryBuilder.queryName());
        return boolQueryBuilder;
    }

    @Override
    public String getQueryName() {
        return MultiMatchQueryBuilder.NAME;
    }

    /**
     * Create a MultiMatchQueryBuilder with only a single field for non-inference indices
     */
    private MultiMatchQueryBuilder createSingleFieldMultiMatch(MultiMatchQueryBuilder original, String fieldName) {
        MultiMatchQueryBuilder singleFieldQuery = new MultiMatchQueryBuilder(original.value());

        // Copy all properties from original query
        singleFieldQuery.type(original.type());
        singleFieldQuery.operator(original.operator());
        singleFieldQuery.analyzer(original.analyzer());
        singleFieldQuery.fuzziness(original.fuzziness());
        singleFieldQuery.prefixLength(original.prefixLength());
        singleFieldQuery.maxExpansions(original.maxExpansions());
        singleFieldQuery.minimumShouldMatch(original.minimumShouldMatch());
        singleFieldQuery.fuzzyRewrite(original.fuzzyRewrite());
        singleFieldQuery.tieBreaker(original.tieBreaker());
        singleFieldQuery.lenient(original.lenient());
        singleFieldQuery.zeroTermsQuery(original.zeroTermsQuery());
        singleFieldQuery.autoGenerateSynonymsPhraseQuery(original.autoGenerateSynonymsPhraseQuery());
        singleFieldQuery.fuzzyTranspositions(original.fuzzyTranspositions());

        // Add only the specific field (without boost for now)
        singleFieldQuery.field(fieldName);

        return singleFieldQuery;
    }
}
