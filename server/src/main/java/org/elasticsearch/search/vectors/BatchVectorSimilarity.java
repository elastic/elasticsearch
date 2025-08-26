/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.VectorSimilarityFunction;

import java.util.Map;

public final class BatchVectorSimilarity {

    private BatchVectorSimilarity() {
    }

    public static float[] computeBatchSimilarity(float[] queryVector, Map<Integer, float[]> docVectors,
                                               int[] docIds, VectorSimilarityFunction function) {
        float[] results = new float[docIds.length];
        float[][] data = organizeSIMDVectors(docVectors, docIds);

        for (int i = 0, l = data.length; i < l; i++) {
            float[] docVector = data[i];
            results[i] = function.compare(queryVector, docVector);
        }

        return results;
    }

    public static float[][] organizeSIMDVectors(Map<Integer, float[]> vectorMap, int[] docIds) {
        float[][] vectors = new float[docIds.length][];
        for (int i = 0; i < docIds.length; i++) {
            vectors[i] = vectorMap.get(docIds[i]);
        }
        return vectors;
    }
}
