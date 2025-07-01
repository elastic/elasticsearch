/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class DenseEmbeddingUtils {

    public static byte[] toBitArray(int position, int length) {
        byte[] bits = new byte[length];
        for (int i = 0; i < 32 && i < length; i++) {
            bits[length - 1 - i] = (byte) ((position >> i) & 1);
        }
        return bits;
    }

    public static byte[] toByteArray(int position, int length) {
        byte[] digits = new byte[length];
        for (int i = length - 1; i >= 0; i--) {
            digits[i] = (byte) (position % 10);
            position /= 10;
        }

        return digits;
    }

    public static float[] toFloatArray(int position, int length) {
        float[] floats = new float[length];
        floats[0] = position;
        return floats;
    }

    public static InferenceAction.Response inferenceResponse(
        int startPosition,
        int size,
        Class<? extends TextEmbeddingResults<?>> embeddingType,
        int dimensions
    ) {
        return new InferenceAction.Response(
            inferenceResults(IntStream.range(startPosition, startPosition + size).toArray(), embeddingType, dimensions)
        );
    }

    public static TextEmbeddingResults<?> inferenceResults(
        int[] values,
        Class<? extends TextEmbeddingResults<?>> embeddingType,
        int dimensions
    ) {
        if (embeddingType.equals(TextEmbeddingBitResults.class)) {
            List<TextEmbeddingByteResults.Embedding> embeddings = new ArrayList<>();
            for (int i = 0; i < values.length; i++) {
                embeddings.add(new TextEmbeddingByteResults.Embedding(toBitArray(values[i], dimensions)));
            }
            return new TextEmbeddingBitResults(embeddings);
        }

        if (embeddingType.equals(TextEmbeddingByteResults.class)) {
            List<TextEmbeddingByteResults.Embedding> embeddings = new ArrayList<>();
            for (int i = 0; i < values.length; i++) {
                embeddings.add(new TextEmbeddingByteResults.Embedding(toByteArray(values[i], dimensions)));
            }
            return new TextEmbeddingByteResults(embeddings);
        }

        if (embeddingType.equals(TextEmbeddingFloatResults.class)) {
            List<TextEmbeddingFloatResults.Embedding> embeddings = new ArrayList<>();
            for (int i = 0; i < values.length; i++) {
                embeddings.add(new TextEmbeddingFloatResults.Embedding(toFloatArray(values[i], dimensions)));
            }
            return new TextEmbeddingFloatResults(embeddings);
        }

        throw new AssertionError("Unexpected Embedding type [" + embeddingType + "]");
    }
}
