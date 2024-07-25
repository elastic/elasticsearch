/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextEmbeddingFloatResults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults.INFERENCE;
import static org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults.TEXT;

public class InferenceChunkedTextEmbeddingFloatResultsTests extends ESTestCase {
    /**
     * Similar to {@link org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextEmbeddingFloatResults#asMap()} but it converts the
     * embeddings float array into a list of floats to make testing equality easier.
     */
    public static Map<String, Object> asMapWithListsInsteadOfArrays(InferenceChunkedTextEmbeddingFloatResults result) {
        return Map.of(
            InferenceChunkedTextEmbeddingFloatResults.FIELD_NAME,
            result.getChunks()
                .stream()
                .map(InferenceChunkedTextEmbeddingFloatResultsTests::inferenceFloatEmbeddingChunkAsMapWithListsInsteadOfArrays)
                .collect(Collectors.toList())
        );
    }

    /**
     * Similar to {@link MlChunkedTextEmbeddingFloatResults.EmbeddingChunk#asMap()} but it converts the double array into a list of doubles
     * to make testing equality easier.
     */
    public static Map<String, Object> inferenceFloatEmbeddingChunkAsMapWithListsInsteadOfArrays(
        InferenceChunkedTextEmbeddingFloatResults.InferenceFloatEmbeddingChunk chunk
    ) {
        var chunkAsList = new ArrayList<Float>(chunk.embedding().length);
        for (double embedding : chunk.embedding()) {
            chunkAsList.add((float) embedding);
        }
        var map = new HashMap<String, Object>();
        map.put(TEXT, chunk.matchedText());
        map.put(INFERENCE, chunkAsList);
        return map;
    }
}
