/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.test.ESTestCase;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link CentroidInformation} byte-backed factories and widening.
 */
public class CentroidAssignmentsTests extends ESTestCase {

    public void testOfBytesBasic() {
        int dims = 4;
        byte[][] centroids = new byte[][] { { 1, 2, 3, 4 }, { -1, -2, -3, -4 }, { 50, 60, 70, 80 } };
        int[] assignments = new int[] { 0, 1, 2, 0, 1 };
        int[] overspill = new int[] { 1, 0, 1, 2, 0 };

        CentroidInformation<byte[]> ca = CentroidInformation.ofBytes(dims, centroids, assignments, new SoarAssignments(overspill));

        assertEquals(3, ca.numCentroids());
        assertSame(centroids, ca.centroids());
        assertSame(assignments, ca.assignments());
        assertArrayEquals(overspill, ((SoarAssignments) ca.overspillAssignments()).assignments());
        assertNull(ca.centroidSlices());
    }

    public void testOfBytesGlobalCentroid() {
        int dims = 3;
        byte[][] centroids = new byte[][] { { 10, 20, 30 }, { -10, -20, -30 } };
        int[] assignments = new int[] { 0, 1 };
        int[] overspill = new int[0];

        CentroidInformation<byte[]> ca = CentroidInformation.ofBytes(dims, centroids, assignments, new SoarAssignments(overspill));

        float[] globalCentroid = ca.globalCentroid();
        assertNotNull(globalCentroid);
        assertEquals(dims, globalCentroid.length);
        // Global centroid = mean of centroids: (10 + -10)/2 = 0, (20 + -20)/2 = 0, (30 + -30)/2 = 0
        assertEquals(0.0f, globalCentroid[0], 1e-5f);
        assertEquals(0.0f, globalCentroid[1], 1e-5f);
        assertEquals(0.0f, globalCentroid[2], 1e-5f);
    }

    public void testOfBytesGlobalCentroidAsymmetric() {
        int dims = 2;
        byte[][] centroids = new byte[][] { { 10, 20 }, { 30, 40 }, { 50, 60 } };
        int[] assignments = new int[] { 0, 1, 2 };
        int[] overspill = new int[0];

        CentroidInformation<byte[]> ca = CentroidInformation.ofBytes(dims, centroids, assignments, new SoarAssignments(overspill));

        float[] globalCentroid = ca.globalCentroid();
        // Mean: (10+30+50)/3 = 30, (20+40+60)/3 = 40
        assertEquals(30.0f, globalCentroid[0], 1e-5f);
        assertEquals(40.0f, globalCentroid[1], 1e-5f);
    }

    public void testFloatCentroidsWideningFromBytes() {
        int dims = 3;
        byte[][] centroids = new byte[][] { { 127, -128, 0 }, { 1, -1, 42 } };
        int[] assignments = new int[] { 0, 1 };
        int[] overspill = new int[0];

        CentroidInformation<byte[]> ca = CentroidInformation.ofBytes(dims, centroids, assignments, new SoarAssignments(overspill));

        float[][] floatCentroids = ca.floatCentroids();
        assertEquals(2, floatCentroids.length);
        assertEquals(3, floatCentroids[0].length);

        // Verify widening preserves sign and value
        assertEquals(127.0f, floatCentroids[0][0], 0f);
        assertEquals(-128.0f, floatCentroids[0][1], 0f);
        assertEquals(0.0f, floatCentroids[0][2], 0f);
        assertEquals(1.0f, floatCentroids[1][0], 0f);
        assertEquals(-1.0f, floatCentroids[1][1], 0f);
        assertEquals(42.0f, floatCentroids[1][2], 0f);
    }

    public void testFloatCentroidsIdentityForFloatBacked() {
        int dims = 2;
        float[][] centroids = new float[][] { { 1.5f, 2.5f }, { 3.5f, 4.5f } };
        int[] assignments = new int[] { 0, 1 };
        int[] overspill = new int[0];

        CentroidInformation<float[]> ca = new CentroidInformation<>(dims, centroids, assignments, new SoarAssignments(overspill));

        float[][] result = ca.floatCentroids();
        assertSame(centroids, result);
    }

    public void testOfBytesEmptyCentroids() {
        int dims = 4;
        byte[][] centroids = new byte[0][];
        int[] assignments = new int[0];
        int[] overspill = new int[0];

        CentroidInformation<byte[]> ca = CentroidInformation.ofBytes(dims, centroids, assignments, new SoarAssignments(overspill));

        assertEquals(0, ca.numCentroids());
        assertEquals(0, ca.centroids().length);

        float[][] floatCentroids = ca.floatCentroids();
        assertEquals(0, floatCentroids.length);
    }

    public void testOfBytesSingleCentroid() {
        int dims = 5;
        byte[][] centroids = new byte[][] { { 10, 20, 30, 40, 50 } };
        int[] assignments = new int[] { 0, 0, 0 };
        int[] overspill = new int[0];

        CentroidInformation<byte[]> ca = CentroidInformation.ofBytes(dims, centroids, assignments, new SoarAssignments(overspill));

        assertEquals(1, ca.numCentroids());
        // Global centroid = the single centroid itself
        float[] globalCentroid = ca.globalCentroid();
        assertEquals(10.0f, globalCentroid[0], 1e-5f);
        assertEquals(50.0f, globalCentroid[4], 1e-5f);
    }

    public void testOfBytesWithCentroidSlices() {
        int dims = 2;
        byte[][] centroids = new byte[][] { { 1, 2 }, { 3, 4 } };
        int[] assignments = new int[] { 0, 0, 1, 1 };
        int[] overspill = new int[] { 1, 1, 0, 0 };
        CentroidSlices slices = new CentroidSlices(new int[] { 0, 2 }, new int[] { 2, 2 });

        CentroidInformation<byte[]> ca = CentroidInformation.ofBytes(dims, centroids, assignments, new SoarAssignments(overspill), slices);

        assertEquals(2, ca.numCentroids());
        assertSame(slices, ca.centroidSlices());
    }
}
