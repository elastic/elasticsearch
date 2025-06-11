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
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.util.List;
import java.util.Map;

public class SemanticSparseVectorQueryRewriteInterceptor extends SemanticQueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_SPARSE_VECTOR_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_sparse_vector_query_rewrite_interception_supported"
    );

    public SemanticSparseVectorQueryRewriteInterceptor() {}

    @Override
    protected String getFieldName(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof SparseVectorQueryBuilder);
        SparseVectorQueryBuilder sparseVectorQueryBuilder = (SparseVectorQueryBuilder) queryBuilder;
        return sparseVectorQueryBuilder.getFieldName();
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof SparseVectorQueryBuilder);
        SparseVectorQueryBuilder sparseVectorQueryBuilder = (SparseVectorQueryBuilder) queryBuilder;
        return sparseVectorQueryBuilder.getQuery();
    }

    @Override
    protected QueryBuilder buildInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        Map<String, List<String>> inferenceIdsIndices = indexInformation.getInferenceIdsIndices();
        if (inferenceIdsIndices.size() == 1) {
            // Simple case, everything uses the same inference ID
            String searchInferenceId = inferenceIdsIndices.keySet().iterator().next();
            return buildNestedQueryFromSparseVectorQuery(queryBuilder, searchInferenceId);
        } else {
            // Multiple inference IDs, construct a boolean query
            return buildInferenceQueryWithMultipleInferenceIds(queryBuilder, inferenceIdsIndices);
        }
    }

    private QueryBuilder buildInferenceQueryWithMultipleInferenceIds(
        QueryBuilder queryBuilder,
        Map<String, List<String>> inferenceIdsIndices
    ) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        for (String inferenceId : inferenceIdsIndices.keySet()) {
            boolQueryBuilder.should(
                createSubQueryForIndices(
                    inferenceIdsIndices.get(inferenceId),
                    buildNestedQueryFromSparseVectorQuery(queryBuilder, inferenceId)
                )
            );
        }
        return boolQueryBuilder;
    }

    @Override
    protected QueryBuilder buildCombinedInferenceAndNonInferenceQuery(
        QueryBuilder queryBuilder,
        InferenceIndexInformationForField indexInformation
    ) {
        assert (queryBuilder instanceof SparseVectorQueryBuilder);
        SparseVectorQueryBuilder sparseVectorQueryBuilder = (SparseVectorQueryBuilder) queryBuilder;
        Map<String, List<String>> inferenceIdsIndices = indexInformation.getInferenceIdsIndices();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(
            createSubQueryForIndices(
                indexInformation.nonInferenceIndices(),
                createSubQueryForIndices(indexInformation.nonInferenceIndices(), sparseVectorQueryBuilder)
            )
        );
        // We always perform nested subqueries on semantic_text fields, to support
        // sparse_vector queries using query vectors.
        for (String inferenceId : inferenceIdsIndices.keySet()) {
            boolQueryBuilder.should(
                createSubQueryForIndices(
                    inferenceIdsIndices.get(inferenceId),
                    buildNestedQueryFromSparseVectorQuery(sparseVectorQueryBuilder, inferenceId)
                )
            );
        }
        return boolQueryBuilder;
    }

    private QueryBuilder buildNestedQueryFromSparseVectorQuery(QueryBuilder queryBuilder, String searchInferenceId) {
        assert (queryBuilder instanceof SparseVectorQueryBuilder);
        SparseVectorQueryBuilder sparseVectorQueryBuilder = (SparseVectorQueryBuilder) queryBuilder;
        SparseVectorQueryBuilder newSparseVectorQueryBuilder = new SparseVectorQueryBuilder(
            SemanticTextField.getEmbeddingsFieldName(sparseVectorQueryBuilder.getFieldName()),
            sparseVectorQueryBuilder.getQueryVectors(),
            (sparseVectorQueryBuilder.getInferenceId() == null && sparseVectorQueryBuilder.getQuery() != null)
                ? searchInferenceId
                : sparseVectorQueryBuilder.getInferenceId(),
            sparseVectorQueryBuilder.getQuery(),
            sparseVectorQueryBuilder.shouldPruneTokens(),
            sparseVectorQueryBuilder.getTokenPruningConfig()
        );
        newSparseVectorQueryBuilder.boost(sparseVectorQueryBuilder.boost());
        newSparseVectorQueryBuilder.queryName(sparseVectorQueryBuilder.queryName());
        return QueryBuilders.nestedQuery(
            SemanticTextField.getChunksFieldName(sparseVectorQueryBuilder.getFieldName()),
            newSparseVectorQueryBuilder,
            ScoreMode.Max
        );
    }

    @Override
    public String getQueryName() {
        return SparseVectorQueryBuilder.NAME;
    }
}
