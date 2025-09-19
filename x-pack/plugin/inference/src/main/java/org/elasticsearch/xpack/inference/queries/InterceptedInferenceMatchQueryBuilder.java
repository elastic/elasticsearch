/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Map;

public class InterceptedInferenceMatchQueryBuilder extends InterceptedInferenceQueryBuilder<MatchQueryBuilder> {
    public static final String NAME = "intercepted_inference_match";

    @SuppressWarnings("deprecation")
    private static final QueryRewriteInterceptor BWC_INTERCEPTOR = new LegacySemanticMatchQueryRewriteInterceptor();

    public InterceptedInferenceMatchQueryBuilder(MatchQueryBuilder originalQuery) {
        super(originalQuery);
    }

    public InterceptedInferenceMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    private InterceptedInferenceMatchQueryBuilder(
        InterceptedInferenceQueryBuilder<MatchQueryBuilder> other,
        Map<String, InferenceResults> inferenceResultsMap
    ) {
        super(other, inferenceResultsMap);
    }

    @Override
    protected Map<String, Float> getFields() {
        return Map.of(getField(), 1.0f);
    }

    @Override
    protected String getQuery() {
        return (String) originalQuery.value();
    }

    @Override
    protected QueryBuilder doRewriteBwC(QueryRewriteContext queryRewriteContext) {
        QueryBuilder rewritten = this;
        if (queryRewriteContext.getMinTransportVersion().before(TransportVersions.NEW_SEMANTIC_QUERY_INTERCEPTORS)) {
            rewritten = BWC_INTERCEPTOR.interceptAndRewrite(queryRewriteContext, originalQuery);
        }

        return rewritten;
    }

    @Override
    protected QueryBuilder copy(Map<String, InferenceResults> inferenceResultsMap) {
        return new InterceptedInferenceMatchQueryBuilder(this, inferenceResultsMap);
    }

    @Override
    protected QueryBuilder queryFields(
        Map<String, Float> inferenceFields,
        Map<String, Float> nonInferenceFields,
        QueryRewriteContext indexMetadataContext
    ) {
        QueryBuilder rewritten;
        MappedFieldType fieldType = indexMetadataContext.getFieldType(getField());
        if (fieldType == null) {
            rewritten = new MatchNoneQueryBuilder();
        } else if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType) {
            rewritten = new SemanticQueryBuilder(getField(), getQuery(), null, inferenceResultsMap).boost(originalQuery.boost())
                .queryName(originalQuery.queryName());
        } else {
            rewritten = originalQuery;
        }

        return rewritten;
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

    private String getField() {
        return originalQuery.fieldName();
    }
}
