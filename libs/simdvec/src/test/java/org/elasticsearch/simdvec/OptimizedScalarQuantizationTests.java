/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

public class OptimizedScalarQuantizationTests extends BaseVectorizationTests {

    static final OptimizedScalarQuantization defaultOsq = defaultProvider().getVectorScorerFactory().newOptimizedScalarQuantization();
    static final OptimizedScalarQuantization panamaOsq = panamaProvider().getVectorScorerFactory().newOptimizedScalarQuantization();

    public void testCenterAndCalculateStatsDp() {
        int size = random().nextInt(128, 512);
        float delta = 1e-3f * size;
        var vector = new float[size];
        var centroid = new float[size];
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
            centroid[i] = random().nextFloat();
        }
        var centeredLucene = new float[size];
        var statsLucene = new float[6];
        defaultOsq.centerAndCalculateStatsDp(vector, centroid, centeredLucene, statsLucene);
        var centeredPanama = new float[size];
        var statsPanama = new float[6];
        panamaOsq.centerAndCalculateStatsDp(vector, centroid, centeredPanama, statsPanama);
        assertArrayEquals(centeredLucene, centeredPanama, delta);
        assertArrayEquals(statsLucene, statsPanama, delta);
    }

    public void testCenterAndCalculateStatsEuclidean() {
        int size = random().nextInt(128, 512);
        float delta = 1e-3f * size;
        var vector = new float[size];
        var centroid = new float[size];
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
            centroid[i] = random().nextFloat();
        }
        var centeredLucene = new float[size];
        var statsLucene = new float[5];
        defaultOsq.centerAndCalculateStatsEuclidean(vector, centroid, centeredLucene, statsLucene);
        var centeredPanama = new float[size];
        var statsPanama = new float[5];
        panamaOsq.centerAndCalculateStatsEuclidean(vector, centroid, centeredPanama, statsPanama);
        assertArrayEquals(centeredLucene, centeredPanama, delta);
        assertArrayEquals(statsLucene, statsPanama, delta);
    }

    public void testCenterAndCalculateStatsDpByteByteCentroid() {
        int size = random().nextInt(128, 512);
        float delta = 1e-3f * size;
        var vector = new byte[size];
        var centroid = new byte[size];
        random().nextBytes(vector);
        random().nextBytes(centroid);
        // byte[],byte[] via Default
        var centeredBB = new float[size];
        var statsBB = new float[6];
        defaultOsq.centerAndCalculateStatsDp(vector, centroid, centeredBB, statsBB);
        // byte[],byte[] via Panama
        var centeredBBPanama = new float[size];
        var statsBBPanama = new float[6];
        panamaOsq.centerAndCalculateStatsDp(vector, centroid, centeredBBPanama, statsBBPanama);
        assertArrayEquals(centeredBB, centeredBBPanama, delta);
        assertArrayEquals(statsBB, statsBBPanama, delta);
    }

    public void testCenterAndCalculateStatsEuclideanByteByteCentroid() {
        int size = random().nextInt(128, 512);
        float delta = 1e-3f * size;
        var vector = new byte[size];
        var centroid = new byte[size];
        random().nextBytes(vector);
        random().nextBytes(centroid);
        // byte[],byte[] via Default
        var centeredBB = new float[size];
        var statsBB = new float[5];
        defaultOsq.centerAndCalculateStatsEuclidean(vector, centroid, centeredBB, statsBB);
        // byte[],byte[] via Panama
        var centeredBBPanama = new float[size];
        var statsBBPanama = new float[5];
        panamaOsq.centerAndCalculateStatsEuclidean(vector, centroid, centeredBBPanama, statsBBPanama);
        assertArrayEquals(centeredBB, centeredBBPanama, delta);
        assertArrayEquals(statsBB, statsBBPanama, delta);
    }

    public void testCalculateLoss() {
        int size = random().nextInt(128, 512);
        float deltaEps = 1e-5f * size;
        var vector = new float[size];
        var min = Float.MAX_VALUE;
        var max = -Float.MAX_VALUE;
        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
            min = Math.min(min, vector[i]);
            max = Math.max(max, vector[i]);
            float delta = vector[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = vector[i] - vecMean;
            vecVar += delta * delta2;
            norm2 += vector[i] * vector[i];
        }
        vecVar /= size;
        float vecStd = (float) Math.sqrt(vecVar);

        int[] destinationDefault = new int[size];
        int[] destinationPanama = new int[size];
        for (byte bits : new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }) {
            int points = 1 << bits;
            float[] initInterval = new float[2];
            OptimizedScalarQuantizer.initInterval(bits, vecStd, vecMean, min, max, initInterval);
            float expected = defaultOsq.calculateLoss(vector, initInterval[0], initInterval[1], points, norm2, 0.1f, destinationDefault);
            float result = panamaOsq.calculateLoss(vector, initInterval[0], initInterval[1], points, norm2, 0.1f, destinationPanama);
            assertEquals(expected, result, deltaEps);
            assertArrayEquals(destinationDefault, destinationPanama);
        }
    }

    public void testCalculateGridPoints() {
        int size = random().nextInt(128, 512);
        float deltaEps = 1e-5f * size;
        var vector = new float[size];
        var min = Float.MAX_VALUE;
        var max = -Float.MAX_VALUE;
        var norm2 = 0f;
        float vecMean = 0;
        float vecVar = 0;
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
            min = Math.min(min, vector[i]);
            max = Math.max(max, vector[i]);
            float delta = vector[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = vector[i] - vecMean;
            vecVar += delta * delta2;
            norm2 += vector[i] * vector[i];
        }
        vecVar /= size;
        float vecStd = (float) Math.sqrt(vecVar);
        int[] destinationDefault = new int[size];
        int[] destinationPanama = new int[size];
        for (byte bits : new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }) {
            int points = 1 << bits;
            float[] initInterval = new float[2];
            OptimizedScalarQuantizer.initInterval(bits, vecStd, vecMean, min, max, initInterval);
            float[] expected = new float[5];
            defaultOsq.calculateLoss(vector, initInterval[0], initInterval[1], points, norm2, 0.1f, destinationDefault);
            defaultOsq.calculateGridPoints(vector, destinationDefault, points, expected);

            float[] result = new float[5];
            panamaOsq.calculateLoss(vector, initInterval[0], initInterval[1], points, norm2, 0.1f, destinationPanama);
            panamaOsq.calculateGridPoints(vector, destinationPanama, points, result);
            assertArrayEquals(expected, result, deltaEps);
            assertArrayEquals(destinationDefault, destinationPanama);
        }
    }

    public void testQuantizeWithIntervals() {
        int vectorSize = randomIntBetween(1, 2048);
        float[] vector = new float[vectorSize];

        byte bits = (byte) randomIntBetween(1, 8);
        for (int i = 0; i < vectorSize; ++i) {
            vector[i] = random().nextFloat();
        }
        float low = random().nextFloat();
        float high = random().nextFloat();
        if (low > high) {
            float tmp = low;
            low = high;
            high = tmp;
        }
        int[] quantizeExpected = new int[vectorSize];
        int[] quantizeResult = new int[vectorSize];
        var expected = defaultOsq.quantizeWithIntervals(vector, quantizeExpected, low, high, bits);
        var result = panamaOsq.quantizeWithIntervals(vector, quantizeResult, low, high, bits);
        assertArrayEquals(quantizeExpected, quantizeResult);
        assertEquals(expected, result, 0f);
    }
}
