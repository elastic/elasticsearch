/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.util.List;

import static org.elasticsearch.xpack.inference.queries.SemanticQueryInterceptionUtils.InferenceIndexInformationForField;

public class SemanticMatchQueryRewriteInterceptor implements QueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_match_query_rewrite_interception_supported"
    );

    public SemanticMatchQueryRewriteInterceptor() {}

    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof MatchQueryBuilder);
        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
        QueryBuilder rewritten = queryBuilder;
        InferenceIndexInformationForField inferenceIndexInformationForField = SemanticQueryInterceptionUtils.resolveIndicesForField(
            matchQueryBuilder.fieldName(),
            context.getResolvedIndices()
        );

        if (inferenceIndexInformationForField == null || inferenceIndexInformationForField.inferenceIndices().isEmpty()) {
            // No inference fields, return original query
            return rewritten;
        } else if (inferenceIndexInformationForField.nonInferenceIndices().isEmpty() == false) {
            // Combined inference and non inference fields
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.should(
                createSemanticSubQuery(
                    inferenceIndexInformationForField.inferenceIndices(),
                    matchQueryBuilder.fieldName(),
                    (String) matchQueryBuilder.value()
                )
            );
            boolQueryBuilder.should(
                SemanticQueryInterceptionUtils.createSubQueryForIndices(
                    inferenceIndexInformationForField.nonInferenceIndices(),
                    matchQueryBuilder
                )
            );
            rewritten = boolQueryBuilder;
        } else {
            // Only inference fields
            rewritten = new SemanticQueryBuilder(matchQueryBuilder.fieldName(), (String) matchQueryBuilder.value(), false);
        }

        return rewritten;
    }

    @Override
    public String getQueryName() {
        return MatchQueryBuilder.NAME;
    }

    private QueryBuilder createSemanticSubQuery(List<String> indices, String fieldName, String value) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new SemanticQueryBuilder(fieldName, value, true));
        boolQueryBuilder.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, indices));
        return boolQueryBuilder;
    }
}
