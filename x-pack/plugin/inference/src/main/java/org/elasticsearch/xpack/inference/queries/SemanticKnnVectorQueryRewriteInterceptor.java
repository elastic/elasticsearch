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
import org.elasticsearch.index.query.NestedQueryBuilder;
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

    public SemanticKnnVectorQueryRewriteInterceptor() {}

    @Override
    protected String getFieldName(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof KnnVectorQueryBuilder);
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
        return knnVectorQueryBuilder.getFieldName();
    }

    @Override
    protected String getQuery(QueryBuilder queryBuilder) {
        TextEmbeddingQueryVectorBuilder queryVectorBuilder = getTextEmbeddingQueryBuilderFromQuery(queryBuilder);
        return queryVectorBuilder != null ? queryVectorBuilder.getModelText() : null;
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
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
        Map<String, List<String>> inferenceIdsIndices = indexInformation.getInferenceIdsIndices();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(
            createSubQueryForIndices(
                indexInformation.nonInferenceIndices(),
                createSubQueryForIndices(indexInformation.nonInferenceIndices(), knnVectorQueryBuilder)
            )
        );
        // We always perform nested subqueries on semantic_text fields, to support
        // knn queries using query vectors.
        for (String inferenceId : inferenceIdsIndices.keySet()) {
            boolQueryBuilder.should(
                createSubQueryForIndices(
                    inferenceIdsIndices.get(inferenceId),
                    buildNestedQueryFromKnnVectorQuery(knnVectorQueryBuilder, inferenceId)
                )
            );
        }
        return boolQueryBuilder;
    }

    private QueryBuilder buildNestedQueryFromKnnVectorQuery(QueryBuilder queryBuilder, String searchInferenceId) {
        assert (queryBuilder instanceof KnnVectorQueryBuilder);
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
        TextEmbeddingQueryVectorBuilder queryVectorBuilder = getTextEmbeddingQueryBuilderFromQuery(queryBuilder);
        if (queryVectorBuilder != null && queryVectorBuilder.getModelId() == null && searchInferenceId != null) {
            // If the model ID was not specified, we infer the inference ID associated with the semantic_text field.
            queryVectorBuilder = new TextEmbeddingQueryVectorBuilder(searchInferenceId, queryVectorBuilder.getModelText());
        }
        return QueryBuilders.nestedQuery(
            SemanticTextField.getChunksFieldName(knnVectorQueryBuilder.getFieldName()),
            buildNewKnnVectorQuery(
                SemanticTextField.getEmbeddingsFieldName(knnVectorQueryBuilder.getFieldName()),
                knnVectorQueryBuilder,
                queryVectorBuilder
            ),
            ScoreMode.Max
        );
    }

    @Override
    protected QueryBuilder createSubQueryForIndices(Collection<String> indices, QueryBuilder queryBuilder) {
        switch (queryBuilder) {
            case KnnVectorQueryBuilder knnVectorQueryBuilder -> {
                return super.createSubQueryForIndices(indices, addIndexFilterToKnnVectorQuery(indices, knnVectorQueryBuilder));
            }
            case NestedQueryBuilder nestedQueryBuilder -> {
                return super.createSubQueryForIndices(
                    indices,
                    new NestedQueryBuilder(
                        nestedQueryBuilder.path(),
                        addIndexFilterToKnnVectorQuery(indices, nestedQueryBuilder.query()),
                        nestedQueryBuilder.scoreMode()
                    )
                );
            }
            case BoolQueryBuilder boolQueryBuilder -> {
                BoolQueryBuilder newBoolQueryBuilder = new BoolQueryBuilder();
                boolQueryBuilder.must().forEach(q -> newBoolQueryBuilder.must(createSubQueryForIndices(indices, q)));
                boolQueryBuilder.should().forEach(q -> newBoolQueryBuilder.should(createSubQueryForIndices(indices, q)));
                boolQueryBuilder.mustNot().forEach(q -> newBoolQueryBuilder.mustNot(createSubQueryForIndices(indices, q)));
                return newBoolQueryBuilder;
            }
            case null, default -> {
                return super.createSubQueryForIndices(indices, queryBuilder);
            }
        }
    }

    private KnnVectorQueryBuilder addIndexFilterToKnnVectorQuery(Collection<String> indices, QueryBuilder queryBuilder) {
        assert queryBuilder instanceof KnnVectorQueryBuilder;
        KnnVectorQueryBuilder original = (KnnVectorQueryBuilder) queryBuilder;
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

        copy.addFilterQuery(new TermsQueryBuilder(IndexFieldMapper.NAME, indices));
        return copy;
    }

    private TextEmbeddingQueryVectorBuilder getTextEmbeddingQueryBuilderFromQuery(QueryBuilder queryBuilder) {
        assert (queryBuilder instanceof KnnVectorQueryBuilder);
        KnnVectorQueryBuilder knnVectorQueryBuilder = (KnnVectorQueryBuilder) queryBuilder;
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
        if (original.queryVectorBuilder() != null) {
            return new KnnVectorQueryBuilder(
                fieldName,
                queryVectorBuilder,
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
