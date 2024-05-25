/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingChunk;
import org.elasticsearch.xpack.core.inference.results.FloatEmbedding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults.INFERENCE;
import static org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults.TEXT;

public class ChunkedTextEmbeddingFloatResultsTests extends AbstractWireSerializingTestCase<ChunkedTextEmbeddingFloatResults> {

    public static ChunkedTextEmbeddingFloatResults createRandomResults() {
        int numChunks = randomIntBetween(1, 5);
        var chunks = new ArrayList<EmbeddingChunk<FloatEmbedding.FloatArrayWrapper>>(numChunks);

        for (int i = 0; i < numChunks; i++) {
            chunks.add(createRandomChunk());
        }

        return new ChunkedTextEmbeddingFloatResults(chunks);
    }

    private static EmbeddingChunk<FloatEmbedding.FloatArrayWrapper> createRandomChunk() {
        int columns = randomIntBetween(1, 10);
        float[] floats = new float[columns];
        for (int i = 0; i < columns; i++) {
            floats[i] = randomFloat();
        }

        return new EmbeddingChunk<>(randomAlphaOfLength(6), new FloatEmbedding(floats));
    }

    @Override
    protected Writeable.Reader<ChunkedTextEmbeddingFloatResults> instanceReader() {
        return ChunkedTextEmbeddingFloatResults::new;
    }

    @Override
    protected ChunkedTextEmbeddingFloatResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected ChunkedTextEmbeddingFloatResults mutateInstance(ChunkedTextEmbeddingFloatResults instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    /**
     * Similar to {@link ChunkedTextEmbeddingFloatResults#asMap()} but it converts the embeddings double array into a list of doubles to
     * make testing equality easier.
     */
    public static Map<String, Object> asMapWithListsInsteadOfArrays(ChunkedTextEmbeddingFloatResults result) {
        return Map.of(
            ChunkedTextEmbeddingFloatResults.FIELD_NAME,
            result.getChunks()
                .stream()
                .map(ChunkedTextEmbeddingFloatResultsTests::asMapWithListsInsteadOfArrays)
                .collect(Collectors.toList())
        );
    }

    private static Map<String, Object> asMapWithListsInsteadOfArrays(EmbeddingChunk<FloatEmbedding.FloatArrayWrapper> chunk) {
        var map = new HashMap<String, Object>();
        map.put(TEXT, chunk.matchedText());
        List<Float> asList = new ArrayList<>();
        for (var val : chunk.embedding().getEmbedding().getFloats()) {
            asList.add(val);
        }
        map.put(INFERENCE, asList);
        return map;
    }
}
