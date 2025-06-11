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

public class SemanticMatchQueryRewriteInterceptor extends SemanticQueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_match_query_rewrite_interception_supported"
    );

    public SemanticMatchQueryRewriteInterceptor() {}

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
        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        QueryBuilder semanticQueryBuilder = createSemanticSubQuery(
            indexInformation.getInferenceIndices(),
            matchQueryBuilder.fieldName(),
            (String) matchQueryBuilder.value(),
            matchQueryBuilder.boost(),
            matchQueryBuilder.queryName()
        );
        boolQueryBuilder.should(semanticQueryBuilder);
        boolQueryBuilder.should(createSubQueryForIndices(indexInformation.nonInferenceIndices(), matchQueryBuilder));
        return boolQueryBuilder;
    }

    @Override
    public String getQueryName() {
        return MatchQueryBuilder.NAME;
    }
}
