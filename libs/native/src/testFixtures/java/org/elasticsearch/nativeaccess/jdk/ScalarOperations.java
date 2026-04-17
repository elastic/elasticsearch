/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;

public class ScalarOperations {

    public static float cosine(byte[] a, byte[] b) {
        int sum = 0;
        int norm1 = 0;
        int norm2 = 0;

        for (int i = 0; i < a.length; i++) {
            byte elem1 = a[i];
            byte elem2 = b[i];
            sum += elem1 * elem2;
            norm1 += elem1 * elem1;
            norm2 += elem2 * elem2;
        }
        return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
    }

    public static float dotProduct(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    public static float dotProduct(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    public static float squareDistance(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            res += diff * diff;
        }
        return res;
    }

    public static float squareDistance(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            res += diff * diff;
        }
        return res;
    }

    static float similarity(VectorSimilarityFunctions.Function function, byte[] a, byte[] b) {
        return switch (function) {
            case COSINE -> cosine(a, b);
            case DOT_PRODUCT -> dotProduct(a, b);
            case SQUARE_DISTANCE -> squareDistance(a, b);
        };
    }

    static float similarity(VectorSimilarityFunctions.Function function, float[] a, float[] b) {
        return switch (function) {
            case DOT_PRODUCT -> dotProduct(a, b);
            case SQUARE_DISTANCE -> squareDistance(a, b);
            case COSINE -> throw new UnsupportedOperationException("cosine not supported for float");
        };
    }

    @FunctionalInterface
    private interface Similarity<T> {
        float apply(T a, T b);
    }

    static void bulk(VectorSimilarityFunctions.Function function, byte[] query, byte[][] data, float[] scores) {
        Similarity<byte[]> sim = switch (function) {
            case COSINE -> ScalarOperations::cosine;
            case DOT_PRODUCT -> ScalarOperations::dotProduct;
            case SQUARE_DISTANCE -> ScalarOperations::squareDistance;
        };
        for (int i = 0; i < data.length; i++) {
            scores[i] = sim.apply(query, data[i]);
        }
    }

    static void bulk(VectorSimilarityFunctions.Function function, float[] query, float[][] data, float[] scores) {
        Similarity<float[]> sim = switch (function) {
            case DOT_PRODUCT -> ScalarOperations::dotProduct;
            case SQUARE_DISTANCE -> ScalarOperations::squareDistance;
            case COSINE -> throw new UnsupportedOperationException("cosine not supported for float");
        };
        for (int i = 0; i < data.length; i++) {
            scores[i] = sim.apply(query, data[i]);
        }
    }

    static void bulkWithOffsets(VectorSimilarityFunctions.Function function, byte[] query, byte[][] data, int[] offsets, float[] scores) {
        Similarity<byte[]> sim = switch (function) {
            case COSINE -> ScalarOperations::cosine;
            case DOT_PRODUCT -> ScalarOperations::dotProduct;
            case SQUARE_DISTANCE -> ScalarOperations::squareDistance;
        };
        for (int i = 0; i < data.length; i++) {
            scores[i] = sim.apply(query, data[offsets[i]]);
        }
    }

    static void bulkWithOffsets(VectorSimilarityFunctions.Function function, float[] query, float[][] data, int[] offsets, float[] scores) {
        Similarity<float[]> sim = switch (function) {
            case DOT_PRODUCT -> ScalarOperations::dotProduct;
            case SQUARE_DISTANCE -> ScalarOperations::squareDistance;
            case COSINE -> throw new UnsupportedOperationException("cosine not supported for float");
        };
        for (int i = 0; i < data.length; i++) {
            scores[i] = sim.apply(query, data[offsets[i]]);
        }
    }
}
