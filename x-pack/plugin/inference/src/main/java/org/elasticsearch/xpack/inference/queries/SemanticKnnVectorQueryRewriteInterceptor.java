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
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.util.List;
import java.util.Map;

public class SemanticKnnVectorQueryRewriteInterceptor extends SemanticQueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_KNN_VECTOR_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_knn_vector_query_rewrite_interception_supported"
    );

    public SemanticKnnVectorQueryRewriteInterceptor() {}

    @Override
    protected String getFieldName(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof KnnVectorQueryBuilder);
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
        return knnVectorQueryBuilder.getFieldName();
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        throw new UnsupportedOperationException("TODO: Add query vector builder");
    }

    @Override
    protected QueryBuilder buildInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        Map<String, List<String>> inferenceIdsIndices = indexInformation.getInferenceIdsIndices();
        if (inferenceIdsIndices.size() == 1) {
            // Simple case, everything uses the same inference ID
            String searchInferenceId = inferenceIdsIndices.keySet().iterator().next();
            return buildNestedQueryFromKnnVectorQuery(queryBuilder, searchInferenceId);
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
                    buildNestedQueryFromKnnVectorQuery(queryBuilder, inferenceId)
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
        assert (queryBuilder instanceof KnnVectorQueryBuilder);
        KnnVectorQueryBuilder knnVec = (KnnVectorQueryBuilder) queryBuilder;
        Map<String, List<String>> inferenceIdsIndices = indexInformation.getInferenceIdsIndices();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(
            createSubQueryForIndices(
                indexInformation.nonInferenceIndices(),
                createSubQueryForIndices(indexInformation.nonInferenceIndices(), knnVec)
            )
        );
        // We always perform nested subqueries on semantic_text fields, to support
        // sparse_vector queries using query vectors.
        for (String inferenceId : inferenceIdsIndices.keySet()) {
            boolQueryBuilder.should(
                createSubQueryForIndices(inferenceIdsIndices.get(inferenceId), buildNestedQueryFromKnnVectorQuery(knnVec, inferenceId))
            );
        }
        return boolQueryBuilder;
    }

    private QueryBuilder buildNestedQueryFromKnnVectorQuery(QueryBuilder queryBuilder, String searchInferenceId) {
        assert (queryBuilder instanceof KnnVectorQueryBuilder);
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
        return QueryBuilders.nestedQuery(
            SemanticTextField.getChunksFieldName(knnVectorQueryBuilder.getFieldName()),
            buildNewKnnVectorQuery(SemanticTextField.getEmbeddingsFieldName(knnVectorQueryBuilder.getFieldName()), knnVectorQueryBuilder),
            ScoreMode.Max
        );
    }

    private KnnVectorQueryBuilder buildNewKnnVectorQuery(String fieldName, KnnVectorQueryBuilder original) {
        if (original.queryVectorBuilder() != null) {
            return new KnnVectorQueryBuilder(
                fieldName,
                original.queryVectorBuilder(),
                original.k(),
                original.numCands(),
                original.getVectorSimilarity()
            );
        } else {
            return new KnnVectorQueryBuilder(
                fieldName,
                original.queryVector(),
                original.k(),
                original.numCands(),
                original.rescoreVectorBuilder(),
                original.getVectorSimilarity()
            );
        }
    }

    @Override
    public String getQueryName() {
        return KnnVectorQueryBuilder.NAME;
    }
}
