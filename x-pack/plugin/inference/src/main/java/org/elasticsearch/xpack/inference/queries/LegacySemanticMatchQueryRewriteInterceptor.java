/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

@Deprecated
public class LegacySemanticMatchQueryRewriteInterceptor extends LegacySemanticQueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_match_query_rewrite_interception_supported"
    );

    public LegacySemanticMatchQueryRewriteInterceptor() {}

    @Override
    protected String getFieldName(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MatchQueryBuilder);
        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
        return matchQueryBuilder.fieldName();
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MatchQueryBuilder);
        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
        return (String) matchQueryBuilder.value();
    }

    @Override
    protected QueryBuilder buildInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        SemanticQueryBuilder semanticQueryBuilder = new SemanticQueryBuilder(indexInformation.fieldName(), getQuery(queryBuilder), false);
        semanticQueryBuilder.boost(queryBuilder.boost());
        semanticQueryBuilder.queryName(queryBuilder.queryName());
        return semanticQueryBuilder;
    }

    @Override
    protected QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation
    ) {
        assert (queryBuilder instanceof MatchQueryBuilder);
        MatchQueryBuilder originalMatchQueryBuilder = (MatchQueryBuilder) queryBuilder;
        // Create a copy for non-inference fields without boost and _name
        MatchQueryBuilder matchQueryBuilder = copyMatchQueryBuilder(originalMatchQueryBuilder);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(
            createSemanticSubQuery(
                indexInformation.getInferenceIndices(),
                matchQueryBuilder.fieldName(),
                (String) matchQueryBuilder.value()
            )
        );
        boolQueryBuilder.should(createSubQueryForIndices(indexInformation.nonInferenceIndices(), matchQueryBuilder));
        boolQueryBuilder.boost(queryBuilder.boost());
        boolQueryBuilder.queryName(queryBuilder.queryName());
        return boolQueryBuilder;
    }

    @Override
    public String getQueryName() {
        return MatchQueryBuilder.NAME;
    }

    private MatchQueryBuilder copyMatchQueryBuilder(MatchQueryBuilder queryBuilder) {
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(queryBuilder.fieldName(), queryBuilder.value());
        matchQueryBuilder.operator(queryBuilder.operator());
        matchQueryBuilder.prefixLength(queryBuilder.prefixLength());
        matchQueryBuilder.maxExpansions(queryBuilder.maxExpansions());
        matchQueryBuilder.fuzzyTranspositions(queryBuilder.fuzzyTranspositions());
        matchQueryBuilder.lenient(queryBuilder.lenient());
        matchQueryBuilder.zeroTermsQuery(queryBuilder.zeroTermsQuery());
        matchQueryBuilder.analyzer(queryBuilder.analyzer());
        matchQueryBuilder.minimumShouldMatch(queryBuilder.minimumShouldMatch());
        matchQueryBuilder.fuzzyRewrite(queryBuilder.fuzzyRewrite());

        if (queryBuilder.fuzziness() != null) {
            matchQueryBuilder.fuzziness(queryBuilder.fuzziness());
        }

        matchQueryBuilder.autoGenerateSynonymsPhraseQuery(queryBuilder.autoGenerateSynonymsPhraseQuery());
        return matchQueryBuilder;
    }
}
