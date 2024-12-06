/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilderWrapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryRewriteInterceptor;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SemanticMatchQueryRewriteInterceptor implements QueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_MATCH_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_match_query_rewrite_interception_supported"
    );

    public SemanticMatchQueryRewriteInterceptor() {}

    @Override
    public QueryBuilder rewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {

        if (queryBuilder instanceof MatchQueryBuilder == false) {
            return queryBuilder;
        }

        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
        QueryBuilder rewritten = queryBuilder;
        ResolvedIndices resolvedIndices = context.getResolvedIndices();
        if (resolvedIndices != null) {
            Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
            List<String> inferenceIndices = new ArrayList<>();
            List<String> nonInferenceIndices = new ArrayList<>();
            for (IndexMetadata indexMetadata : indexMetadataCollection) {
                String indexName = indexMetadata.getIndex().getName();
                InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(matchQueryBuilder.fieldName());
                if (inferenceFieldMetadata != null) {
                    inferenceIndices.add(indexName);
                } else {
                    nonInferenceIndices.add(indexName);
                }
            }

            if (inferenceIndices.isEmpty()) {
                return rewritten;
            } else if (nonInferenceIndices.isEmpty() == false) {
                BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                for (String inferenceIndexName : inferenceIndices) {
                    // Add a separate clause for each semantic query, because they may be using different inference endpoints
                    boolQueryBuilder.should(
                        createSemanticSubQuery(inferenceIndexName, matchQueryBuilder.fieldName(), (String) matchQueryBuilder.value())
                    );
                }
                boolQueryBuilder.should(createMatchSubQuery(nonInferenceIndices, matchQueryBuilder.fieldName(), matchQueryBuilder.value()));
                rewritten = boolQueryBuilder;
            } else {
                rewritten = new SemanticQueryBuilder(matchQueryBuilder.fieldName(), (String) matchQueryBuilder.value(), false);
            }
        }

        return rewritten;

    }

    @Override
    public String getName() {
        return MatchQueryBuilder.NAME;
    }

    private QueryBuilder createSemanticSubQuery(String indexName, String fieldName, String value) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new SemanticQueryBuilder(fieldName, value, true));
        boolQueryBuilder.filter(new TermQueryBuilder(IndexFieldMapper.NAME, indexName));
        return boolQueryBuilder;
    }

    private QueryBuilder createMatchSubQuery(List<String> indices, String fieldName, Object value) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new AbstractSemanticMatchQueryWrapper(fieldName, value));
        boolQueryBuilder.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, indices));
        return boolQueryBuilder;
    }

    static class AbstractSemanticMatchQueryWrapper extends AbstractQueryBuilderWrapper<MatchQueryBuilder> {
        AbstractSemanticMatchQueryWrapper(String fieldName, Object value) {
            super(new MatchQueryBuilder(fieldName, value));
        }
    }
}
