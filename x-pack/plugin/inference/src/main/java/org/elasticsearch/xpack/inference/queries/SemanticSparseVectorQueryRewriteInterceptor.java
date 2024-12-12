/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;

public class SemanticSparseVectorQueryRewriteInterceptor implements QueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_SPARSE_VECTOR_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_sparse_vector_query_rewrite_interception_supported"
    );

    private static final String NESTED_FIELD_PATH = ".inference.chunks";
    private static final String NESTED_EMBEDDINGS_FIELD = NESTED_FIELD_PATH + ".embeddings";

    public SemanticSparseVectorQueryRewriteInterceptor() {}

    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof SparseVectorQueryBuilder);
        SparseVectorQueryBuilder sparseVectorQueryBuilder = (SparseVectorQueryBuilder) queryBuilder;
        QueryBuilder rewritten = queryBuilder;
        SemanticQueryInterceptionUtils.SemanticTextIndexInformationForField semanticTextIndexInformationForField =
            SemanticQueryInterceptionUtils.resolveIndicesForField(sparseVectorQueryBuilder.getFieldName(), context.getResolvedIndices());

        if (semanticTextIndexInformationForField == null || semanticTextIndexInformationForField.semanticMappedIndices().isEmpty()) {
            // No semantic text fields, return original query
            return rewritten;
        } else if (semanticTextIndexInformationForField.otherIndices().isEmpty() == false) {
            // Combined semantic and sparse vector fields
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            // sparse_vector fields should be passed in as their own clause
            boolQueryBuilder.should(
                SemanticQueryInterceptionUtils.createSubQueryForIndices(
                    semanticTextIndexInformationForField.otherIndices(),
                    SemanticQueryInterceptionUtils.createSubQueryForIndices(
                        semanticTextIndexInformationForField.otherIndices(),
                        sparseVectorQueryBuilder
                    )
                )
            );
            // semantic text fields should be passed in as nested sub queries
            boolQueryBuilder.should(
                SemanticQueryInterceptionUtils.createSubQueryForIndices(
                    semanticTextIndexInformationForField.semanticMappedIndices(),
                    buildNestedQueryFromSparseVectorQuery(sparseVectorQueryBuilder)
                )

            );

            rewritten = boolQueryBuilder;
        } else {
            // Only semantic text fields
            rewritten = buildNestedQueryFromSparseVectorQuery(sparseVectorQueryBuilder);
        }

        return rewritten;
    }

    private QueryBuilder buildNestedQueryFromSparseVectorQuery(SparseVectorQueryBuilder sparseVectorQueryBuilder) {
        return QueryBuilders.nestedQuery(
            getNestedFieldPath(sparseVectorQueryBuilder.getFieldName()),
            new SparseVectorQueryBuilder(
                getNestedEmbeddingsField(sparseVectorQueryBuilder.getFieldName()),
                sparseVectorQueryBuilder.getQueryVectors(),
                sparseVectorQueryBuilder.getInferenceId(),
                sparseVectorQueryBuilder.getQuery(),
                sparseVectorQueryBuilder.shouldPruneTokens(),
                sparseVectorQueryBuilder.getTokenPruningConfig()
            ),
            ScoreMode.Max
        );
    }

    private static String getNestedFieldPath(String fieldName) {
        return fieldName + NESTED_FIELD_PATH;
    }

    private static String getNestedEmbeddingsField(String fieldName) {
        return fieldName + NESTED_EMBEDDINGS_FIELD;
    }

    @Override
    public String getQueryName() {
        return SparseVectorQueryBuilder.NAME;
    }
}
