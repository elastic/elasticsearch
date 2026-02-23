/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.test.ESTestCase;

public class RandomProjectionTests extends ESTestCase {

    public void testProjectReducesDimension() {
        int originalDim = 100;
        int projectedDim = 10;
        long seed = 42L;

        RandomProjection projection = new RandomProjection(originalDim, projectedDim, seed);
        float[] input = new float[originalDim];
        for (int i = 0; i < originalDim; i++) {
            input[i] = randomFloat();
        }

        float[] result = projection.project(input);
        assertEquals(projectedDim, result.length);
    }

    public void testDeterministicWithSameSeed() {
        int originalDim = 50;
        int projectedDim = 10;
        long seed = 123L;

        float[] input = new float[originalDim];
        for (int i = 0; i < originalDim; i++) {
            input[i] = (float) i;
        }

        RandomProjection p1 = new RandomProjection(originalDim, projectedDim, seed);
        RandomProjection p2 = new RandomProjection(originalDim, projectedDim, seed);

        float[] r1 = p1.project(input);
        float[] r2 = p2.project(input);

        assertArrayEquals(r1, r2, 0.0f);
    }

    public void testDifferentSeedsProduceDifferentResults() {
        int originalDim = 50;
        int projectedDim = 10;

        float[] input = new float[originalDim];
        for (int i = 0; i < originalDim; i++) {
            input[i] = (float) i;
        }

        RandomProjection p1 = new RandomProjection(originalDim, projectedDim, 1L);
        RandomProjection p2 = new RandomProjection(originalDim, projectedDim, 2L);

        float[] r1 = p1.project(input);
        float[] r2 = p2.project(input);

        boolean allEqual = true;
        for (int i = 0; i < projectedDim; i++) {
            if (r1[i] != r2[i]) {
                allEqual = false;
                break;
            }
        }
        assertFalse("Different seeds should produce different projections", allEqual);
    }

    public void testApproximateDistancePreservation() {
        int originalDim = 200;
        int projectedDim = 50;
        long seed = 42L;

        RandomProjection projection = new RandomProjection(originalDim, projectedDim, seed);

        float[] a = new float[originalDim];
        float[] b = new float[originalDim];
        for (int i = 0; i < originalDim; i++) {
            a[i] = randomFloat() * 10;
            b[i] = randomFloat() * 10;
        }

        double originalDist = 0;
        for (int i = 0; i < originalDim; i++) {
            double diff = a[i] - b[i];
            originalDist += diff * diff;
        }

        float[] pa = projection.project(a);
        float[] pb = projection.project(b);
        double projectedDist = 0;
        for (int i = 0; i < projectedDim; i++) {
            double diff = pa[i] - pb[i];
            projectedDist += diff * diff;
        }

        double ratio = projectedDist / originalDist;
        assertTrue("Distance ratio should be reasonable, got " + ratio, ratio > 0.01 && ratio < 100);
    }

    public void testProjectionDimCannotExceedOriginal() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RandomProjection(10, 20, 42L));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("projectedDim [20] must be <= originalDim [10]"));
    }

    public void testProjectBatch() {
        int originalDim = 30;
        int projectedDim = 5;
        long seed = 99L;

        RandomProjection projection = new RandomProjection(originalDim, projectedDim, seed);
        float[][] vectors = new float[10][originalDim];
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < originalDim; j++) {
                vectors[i][j] = randomFloat();
            }
        }

        float[][] results = projection.projectBatch(vectors);
        assertEquals(10, results.length);
        for (float[] result : results) {
            assertEquals(projectedDim, result.length);
        }
    }

    public void testIdentityWhenDimsEqual() {
        int dim = 5;
        long seed = 42L;

        RandomProjection projection = new RandomProjection(dim, dim, seed);
        float[] input = new float[] { 1, 2, 3, 4, 5 };
        float[] result = projection.project(input);
        assertEquals(dim, result.length);
    }
}
