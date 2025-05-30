/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

public class SemanticMatchAllQueryRewriteInterceptor extends SemanticQueryRewriteInterceptor{

    public static final NodeFeature SEMANTIC_MATCH_ALL_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_match_all_query_rewrite_interception_supported"
    );

    public SemanticMatchAllQueryRewriteInterceptor() {}

    @Override
    protected String getFieldName(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MatchAllQueryBuilder);
        return null; // MatchAllQueryBuilder does not have a field name, it matches all documents
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        return "*"; // MatchAllQueryBuilder does not have a specific query, it matches all documents
    }

    @Override
    protected QueryBuilder buildInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        return new SemanticQueryBuilder(indexInformation.fieldName(), getQuery(queryBuilder), false);
    }

    @Override
    protected QueryBuilder buildCombinedInferenceAndNonInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        assert (queryBuilder instanceof MatchAllQueryBuilder);
        MatchAllQueryBuilder matchAllQueryBuilder = (MatchAllQueryBuilder) queryBuilder;
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.should(
            createSemanticSubQuery(
                indexInformation.getInferenceIndices(),
                indexInformation.fieldName(),
                getQuery(queryBuilder)
            )
        );
        boolQueryBuilder.should(createSubQueryForIndices(indexInformation.nonInferenceIndices(), matchAllQueryBuilder));
        return boolQueryBuilder;
    }

    @Override
    public String getQueryName() {
        return MatchAllQueryBuilder.NAME;
    }
}
