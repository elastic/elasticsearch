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
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import static org.elasticsearch.xpack.inference.queries.SemanticQueryInterceptionUtils.InferenceIndexInformationForField;

public class SemanticSparseVectorQueryRewriteInterceptor implements QueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_SPARSE_VECTOR_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_sparse_vector_query_rewrite_interception_supported"
    );

    public SemanticSparseVectorQueryRewriteInterceptor() {}

    @Override
    public QueryBuilder interceptAndRewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof SparseVectorQueryBuilder);
        SparseVectorQueryBuilder sparseVectorQueryBuilder = (SparseVectorQueryBuilder) queryBuilder;
        QueryBuilder rewritten = queryBuilder;
        InferenceIndexInformationForField inferenceIndexInformationForField = SemanticQueryInterceptionUtils.resolveIndicesForField(
            sparseVectorQueryBuilder.getFieldName(),
            context.getResolvedIndices()
        );

        if (inferenceIndexInformationForField == null || inferenceIndexInformationForField.inferenceIndices().isEmpty()) {
            // No inference fields, return original query
            return rewritten;
        } else if (inferenceIndexInformationForField.nonInferenceIndices().isEmpty() == false) {
            // Combined inference and non inference fields
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.should(
                SemanticQueryInterceptionUtils.createSubQueryForIndices(
                    inferenceIndexInformationForField.nonInferenceIndices(),
                    SemanticQueryInterceptionUtils.createSubQueryForIndices(
                        inferenceIndexInformationForField.nonInferenceIndices(),
                        sparseVectorQueryBuilder
                    )
                )
            );
            // We always perform nested subqueries on semantic_text fields, to support
            // sparse_vector queries using query vectors
            boolQueryBuilder.should(
                SemanticQueryInterceptionUtils.createSubQueryForIndices(
                    inferenceIndexInformationForField.inferenceIndices(),
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
        return fieldName + SemanticTextField.INFERENCE_FIELD + SemanticTextField.CHUNKS_FIELD;
    }

    private static String getNestedEmbeddingsField(String fieldName) {
        return getNestedFieldPath(fieldName) + SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
    }

    @Override
    public String getQueryName() {
        return SparseVectorQueryBuilder.NAME;
    }
}
