/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class SemanticQuery extends Query {
    private final String name;
    private final String text;
    private final InferenceResults inferenceResults;

    public SemanticQuery(Source source, String name, String text, InferenceResults inferenceResults) {
        super(source);
        this.name = name;
        this.text = text;
        this.inferenceResults = inferenceResults;
    }

    @Override
    public QueryBuilder asBuilder() {
        QueryBuilder childQueryBuilder;

        if (inferenceResults instanceof TextExpansionResults) {
            childQueryBuilder = textExpansionQueryBuilder((TextExpansionResults) inferenceResults);
        } else if (inferenceResults instanceof MlTextEmbeddingResults) {
            childQueryBuilder = knnQueryBuilder((MlTextEmbeddingResults) inferenceResults);
        } else {

            // This should never happen, but we handle it here either way
            throw new IllegalStateException("Cannot handle inference results");
        }

        String nestedFieldPath = name.concat(".inference.chunks");
        return new NestedQueryBuilder(nestedFieldPath, childQueryBuilder, ScoreMode.Max);
    }

    @Override
    protected String innerToString() {
        return null;
    }

    private QueryBuilder textExpansionQueryBuilder(TextExpansionResults textExpansionResults) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (var weightedToken : textExpansionResults.getWeightedTokens()) {
            boolQuery.should(QueryBuilders.termQuery(embeddingsFieldName(), weightedToken.token()).boost(weightedToken.weight()));
        }
        boolQuery.minimumShouldMatch(1);
        return boolQuery;
    }

    private QueryBuilder knnQueryBuilder(MlTextEmbeddingResults textEmbeddingResults) {
        float[] inference = textEmbeddingResults.getInferenceAsFloat();
        return new KnnVectorQueryBuilder(embeddingsFieldName(), inference, null, null, null);
    }

    private String embeddingsFieldName() {
        return name.concat(".inference.chunks.embeddings");
    }
}
