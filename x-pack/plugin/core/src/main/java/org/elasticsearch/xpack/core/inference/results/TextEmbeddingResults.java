/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Writes a text embedding result in the follow json format
 * {
 *     "text_embedding": [
 *         {
 *             "embedding": [
 *                 0.1
 *             ]
 *         },
 *         {
 *             "embedding": [
 *                 0.2
 *             ]
 *         }
 *     ]
 * }
 */
public record TextEmbeddingResults(List<FloatEmbedding> embeddings) implements InferenceServiceResults, TextEmbedding, EmbeddingResults {
    public static final String NAME = "text_embedding_service_results";
    public static final String TEXT_EMBEDDING = TaskType.TEXT_EMBEDDING.toString();

    public TextEmbeddingResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(FloatEmbedding::new));
    }

    @SuppressWarnings("deprecation")
    TextEmbeddingResults(LegacyTextEmbeddingResults legacyTextEmbeddingResults) {
        this(
            legacyTextEmbeddingResults.embeddings()
                .stream()
                .map(embedding -> new FloatEmbedding(embedding.values()))
                .collect(Collectors.toList())
        );
    }

    public static TextEmbeddingResults of(List<? extends InferenceResults> results) {
        List<FloatEmbedding> embeddings = new ArrayList<>(results.size());
        for (InferenceResults result : results) {
            if (result instanceof org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults embeddingResult) {
                embeddings.add(FloatEmbedding.of(embeddingResult));
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
                    "Received invalid inference result, of type " + result.getClass().getName() + " but expected TextEmbeddingResults."
                );
            }
        }
        return new TextEmbeddingResults(embeddings);
    }

    @Override
    public int getFirstEmbeddingSize() {
        return TextEmbeddingUtils.getFirstEmbeddingSize(new ArrayList<>(embeddings));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TEXT_EMBEDDING);
        for (Embedding<?> embedding : embeddings) {
            embedding.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embeddings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return embeddings.stream()
            .map(
                embedding -> new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                    TEXT_EMBEDDING,
                    embedding.asDoubleArray(),
                    false
                )
            )
            .toList();
    }

    @Override
    @SuppressWarnings("deprecation")
    public List<? extends InferenceResults> transformToLegacyFormat() {
        var legacyEmbedding = new LegacyTextEmbeddingResults(
            embeddings.stream().map(embedding -> new LegacyTextEmbeddingResults.Embedding(embedding.asFloatArray())).toList()
        );

        return List.of(legacyEmbedding);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(TEXT_EMBEDDING, embeddings);

        return map;
    }

    @Override
    public List<FloatEmbedding> embeddings() {
        return embeddings;
    }

    @Override
    public EmbeddingType embeddingType() {
        return EmbeddingType.FLOAT;
    }
}
