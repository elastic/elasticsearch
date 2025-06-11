/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SemanticKnnVectorQueryRewriteInterceptor extends SemanticQueryRewriteInterceptor {

    public static final NodeFeature SEMANTIC_KNN_VECTOR_QUERY_REWRITE_INTERCEPTION_SUPPORTED = new NodeFeature(
        "search.semantic_knn_vector_query_rewrite_interception_supported"
    );
    public static final NodeFeature SEMANTIC_KNN_FILTER_FIX = new NodeFeature("search.semantic_knn_filter_fix");

    public SemanticKnnVectorQueryRewriteInterceptor() {}

    @Override
    protected String getFieldName(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof KnnVectorQueryBuilder);
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
        return knnVectorQueryBuilder.getFieldName();
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof KnnVectorQueryBuilder);
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
        TextEmbeddingQueryVectorBuilder queryVectorBuilder = getTextEmbeddingQueryBuilderFromQuery(knnVectorQueryBuilder);
        return queryVectorBuilder != null ? queryVectorBuilder.getModelText() : null;
    }

    @Override
    protected QueryBuilder buildInferenceQuery(QueryBuilder queryBuilder, InferenceIndexInformationForField indexInformation) {
        assert (queryBuilder instanceof KnnVectorQueryBuilder);
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
        Map<String, List<String>> inferenceIdsIndices = indexInformation.getInferenceIdsIndices();
        if (inferenceIdsIndices.size() == 1) {
            // Simple case, everything uses the same inference ID
            Map.Entry<String, List<String>> inferenceIdIndex = inferenceIdsIndices.entrySet().iterator().next();
            String searchInferenceId = inferenceIdIndex.getKey();
            List<String> indices = inferenceIdIndex.getValue();
            return buildNestedQueryFromKnnVectorQuery(knnVectorQueryBuilder, indices, searchInferenceId);
        } else {
            // Multiple inference IDs, construct a boolean query
            return buildInferenceQueryWithMultipleInferenceIds(knnVectorQueryBuilder, inferenceIdsIndices);
        }
    }

    private QueryBuilder buildInferenceQueryWithMultipleInferenceIds(
        KnnVectorQueryBuilder queryBuilder,
        Map<String, List<String>> inferenceIdsIndices
    ) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        for (String inferenceId : inferenceIdsIndices.keySet()) {
            boolQueryBuilder.should(
                createSubQueryForIndices(
                    inferenceIdsIndices.get(inferenceId),
                    buildNestedQueryFromKnnVectorQuery(queryBuilder, inferenceIdsIndices.get(inferenceId), inferenceId)
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
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
        Map<String, List<String>> inferenceIdsIndices = indexInformation.getInferenceIdsIndices();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(addIndexFilterToKnnVectorQuery(indexInformation.nonInferenceIndices(), knnVectorQueryBuilder));

        // We always perform nested subqueries on semantic_text fields, to support knn queries using query vectors.
        // Both pre and post filtering are required here to ensure we get the results we need without errors based on field types.
        for (String inferenceId : inferenceIdsIndices.keySet()) {
            boolQueryBuilder.should(
                createSubQueryForIndices(
                    inferenceIdsIndices.get(inferenceId),
                    buildNestedQueryFromKnnVectorQuery(knnVectorQueryBuilder, inferenceIdsIndices.get(inferenceId), inferenceId)
                )
            );
        }
        return boolQueryBuilder;
    }

    private QueryBuilder buildNestedQueryFromKnnVectorQuery(
        KnnVectorQueryBuilder knnVectorQueryBuilder,
        List<String> indices,
        String searchInferenceId
    ) {
        KnnVectorQueryBuilder filteredKnnVectorQueryBuilder = addIndexFilterToKnnVectorQuery(indices, knnVectorQueryBuilder);
        TextEmbeddingQueryVectorBuilder queryVectorBuilder = getTextEmbeddingQueryBuilderFromQuery(filteredKnnVectorQueryBuilder);
        if (queryVectorBuilder != null && queryVectorBuilder.getModelId() == null && searchInferenceId != null) {
            // If the model ID was not specified, we infer the inference ID associated with the semantic_text field.
            queryVectorBuilder = new TextEmbeddingQueryVectorBuilder(searchInferenceId, queryVectorBuilder.getModelText());
        }
        return QueryBuilders.nestedQuery(
            SemanticTextField.getChunksFieldName(filteredKnnVectorQueryBuilder.getFieldName()),
            buildNewKnnVectorQuery(
                SemanticTextField.getEmbeddingsFieldName(filteredKnnVectorQueryBuilder.getFieldName()),
                filteredKnnVectorQueryBuilder,
                queryVectorBuilder
            ),
            ScoreMode.Max
        );
    }

    private KnnVectorQueryBuilder addIndexFilterToKnnVectorQuery(Collection<String> indices, KnnVectorQueryBuilder original) {
        KnnVectorQueryBuilder copy;
        if (original.queryVectorBuilder() != null) {
            copy = new KnnVectorQueryBuilder(
                original.getFieldName(),
                original.queryVectorBuilder(),
                original.k(),
                original.numCands(),
                original.getVectorSimilarity()
            );
        } else {
            copy = new KnnVectorQueryBuilder(
                original.getFieldName(),
                original.queryVector(),
                original.k(),
                original.numCands(),
                original.rescoreVectorBuilder(),
                original.getVectorSimilarity()
            );
        }

        copy.boost(original.boost());
        copy.queryName(original.queryName());
        copy.addFilterQueries(original.filterQueries());
        copy.addFilterQuery(new TermsQueryBuilder(IndexFieldMapper.NAME, indices));
        return copy;
    }

    private TextEmbeddingQueryVectorBuilder getTextEmbeddingQueryBuilderFromQuery(KnnVectorQueryBuilder knnVectorQueryBuilder) {
        QueryVectorBuilder queryVectorBuilder = knnVectorQueryBuilder.queryVectorBuilder();
        if (queryVectorBuilder == null) {
            return null;
        }
        assert (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder);
        return (TextEmbeddingQueryVectorBuilder) queryVectorBuilder;
    }

    private KnnVectorQueryBuilder buildNewKnnVectorQuery(
        String fieldName,
        KnnVectorQueryBuilder original,
        QueryVectorBuilder queryVectorBuilder
    ) {
        KnnVectorQueryBuilder newQueryBuilder;
        if (original.queryVectorBuilder() != null) {
            newQueryBuilder = new KnnVectorQueryBuilder(
                fieldName,
                queryVectorBuilder,
                original.k(),
                original.numCands(),
                original.getVectorSimilarity()
            );
        } else {
            newQueryBuilder = new KnnVectorQueryBuilder(
                fieldName,
                original.queryVector(),
                original.k(),
                original.numCands(),
                original.rescoreVectorBuilder(),
                original.getVectorSimilarity()
            );
        }

        newQueryBuilder.boost(original.boost());
        newQueryBuilder.queryName(original.queryName());
        newQueryBuilder.addFilterQueries(original.filterQueries());
        return newQueryBuilder;
    }

    @Override
    public String getQueryName() {
        return KnnVectorQueryBuilder.NAME;
    }
}
