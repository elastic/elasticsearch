/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;

public class InterceptedKnnQueryBuilder extends InterceptedQueryBuilder<KnnVectorQueryBuilder> {
    public static final String NAME = "intercepted_knn";

    public InterceptedKnnQueryBuilder(KnnVectorQueryBuilder queryBuilder) {
        super(queryBuilder);
    }

    public InterceptedKnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public InterceptedKnnQueryBuilder(InterceptedQueryBuilder<KnnVectorQueryBuilder> other, EmbeddingsProvider embeddingsProvider) {
        super(other, embeddingsProvider);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected String getFieldName() {
        return originalQuery.getFieldName();
    }

    @Override
    protected String getQuery() {
        String query = null;
        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder textEmbeddingQueryVectorBuilder) {
            query = textEmbeddingQueryVectorBuilder.getModelText();
        }

        return query;
    }

    @Override
    protected QueryBuilder copy(EmbeddingsProvider embeddingsProvider) {
        return new InterceptedKnnQueryBuilder(this, embeddingsProvider);
    }

    @Override
    protected QueryBuilder querySemanticTextField(SemanticTextFieldMapper.SemanticTextFieldType semanticTextField) {
        QueryVectorBuilder queryVectorBuilder = originalQuery.queryVectorBuilder();
        if (queryVectorBuilder instanceof TextEmbeddingQueryVectorBuilder textEmbeddingQueryVectorBuilder
            && textEmbeddingQueryVectorBuilder.getModelId() == null) {
            // If the model ID was not specified, we infer the inference ID associated with the semantic_text field
            queryVectorBuilder = new TextEmbeddingQueryVectorBuilder(
                semanticTextField.getSearchInferenceId(),
                textEmbeddingQueryVectorBuilder.getModelText()
            );
        }

        String embeddingsFieldName = SemanticTextField.getEmbeddingsFieldName(getFieldName());
        KnnVectorQueryBuilder innerKnnQuery;
        if (queryVectorBuilder != null) {
            innerKnnQuery = new KnnVectorQueryBuilder(
                embeddingsFieldName,
                queryVectorBuilder,
                originalQuery.k(),
                originalQuery.numCands(),
                originalQuery.getVectorSimilarity()
            );
        } else {
            innerKnnQuery = new KnnVectorQueryBuilder(
                embeddingsFieldName,
                originalQuery.queryVector(),
                originalQuery.k(),
                originalQuery.numCands(),
                originalQuery.rescoreVectorBuilder(),
                originalQuery.getVectorSimilarity()
            );
        }
        innerKnnQuery.addFilterQueries(originalQuery.filterQueries());

        return QueryBuilders.nestedQuery(
            SemanticTextField.getChunksFieldName(getFieldName()),
            innerKnnQuery,
            ScoreMode.Max
        ).boost(originalQuery.boost()).queryName(originalQuery.queryName());
    }
}
