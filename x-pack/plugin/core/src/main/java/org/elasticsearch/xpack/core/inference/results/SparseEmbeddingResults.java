/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public record SparseEmbeddingResults(List<SparseEmbedding> embeddings) implements InferenceServiceResults, EmbeddingResults {

    public static final String NAME = "sparse_embedding_results";
    public static final String SPARSE_EMBEDDING = TaskType.SPARSE_EMBEDDING.toString();

    public SparseEmbeddingResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(SparseEmbedding::new));
    }

    public static SparseEmbeddingResults of(List<? extends InferenceResults> results) {
        List<SparseEmbedding> embeddings = new ArrayList<>(results.size());

        for (InferenceResults result : results) {
            if (result instanceof TextExpansionResults expansionResults) {
                embeddings.add(SparseEmbedding.create(expansionResults.getWeightedTokens(), expansionResults.isTruncated()));
            } else if (result instanceof org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults errorResult) {
                if (errorResult.getException() instanceof ElasticsearchStatusException statusException) {
                    throw statusException;
                } else {
                    throw new ElasticsearchStatusException(
                        "Received error inference result.",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        errorResult.getException()
                    );
                }
            } else {
                throw new IllegalArgumentException(
                    "Received invalid legacy inference result, of type "
                        + result.getClass().getName()
                        + " but expected SparseEmbeddingResults."
                );
            }
        }

        return new SparseEmbeddingResults(embeddings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(SPARSE_EMBEDDING);

        for (var embedding : embeddings) {
            embedding.toXContent(builder, params);
        }

        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embeddings);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        var embeddingList = embeddings.stream().map(Embedding::asMap).toList();

        map.put(SPARSE_EMBEDDING, embeddingList);
        return map;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return transformToLegacyFormat();
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        return embeddings.stream()
            .map(
                embedding -> new TextExpansionResults(
                    DEFAULT_RESULTS_FIELD,
                    embedding.getEmbedding()
                        .stream()
                        .map(weightedToken -> new WeightedToken(weightedToken.token(), weightedToken.weight()))
                        .toList(),
                    embedding.isTruncated()
                )
            )
            .toList();
    }

    @Override
    public EmbeddingType embeddingType() {
        return EmbeddingType.SPARSE;
    }
}
