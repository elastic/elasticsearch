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
    protected Map<String, Float> getFieldNamesWithBoosts(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
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
    protected QueryBuilder buildInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation,
        Float fieldWBoost
    ) {
        SemanticQueryBuilder semanticQueryBuilder = new SemanticQueryBuilder(indexInformation.fieldName(), getQuery(queryBuilder), false);
        semanticQueryBuilder.boost(queryBuilder.boost() * fieldWBoost);
        semanticQueryBuilder.queryName(queryBuilder.queryName());
        return semanticQueryBuilder;
    }

    @Override
    protected QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation,
        Float fieldBoost
    ) {
        assert (queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQueryBuilder = (MultiMatchQueryBuilder) queryBuilder;

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(
            createSemanticSubQuery(
                indexInformation.getInferenceIndices(),
                indexInformation.fieldName(),
                (String) multiMatchQueryBuilder.value()
            )
        );

        boolQueryBuilder.should(
            createMatchSubQuery(
                indexInformation.nonInferenceIndices(),
                indexInformation.fieldName(),
                (String) multiMatchQueryBuilder.value()
            )
        );

        boolQueryBuilder.boost(queryBuilder.boost() * fieldBoost);
        boolQueryBuilder.queryName(queryBuilder.queryName());
        return boolQueryBuilder;
    }

    @Override
    public String getQueryName() {
        return MultiMatchQueryBuilder.NAME;
    }
}
