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
 * Tests for {@link CentroidInformation} factories (float and byte-backed).
 */
public class CentroidInformationTests extends ESTestCase {

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

    public void testFloatConstructorBasic() {
        int dims = 2;
        float[][] centroids = new float[][] { { 1.5f, 2.5f }, { 3.5f, 4.5f } };
        int[] assignments = new int[] { 0, 1, 0 };
        int[] overspill = new int[] { 1, 0, 1 };

        CentroidInformation<float[]> ci = new CentroidInformation<>(dims, centroids, assignments, new SoarAssignments(overspill));

        assertEquals(2, ci.numCentroids());
        assertSame(centroids, ci.centroids());
        assertSame(assignments, ci.assignments());
        assertArrayEquals(overspill, ((SoarAssignments) ci.overspillAssignments()).assignments());
        assertNull(ci.centroidSlices());
        // Global centroid = mean: (1.5+3.5)/2=2.5, (2.5+4.5)/2=3.5
        assertEquals(2.5f, ci.globalCentroid()[0], 1e-5f);
        assertEquals(3.5f, ci.globalCentroid()[1], 1e-5f);
    }

    public void testFloatConstructorWithSlices() {
        int dims = 3;
        float[][] centroids = new float[][] { { 1f, 2f, 3f }, { 4f, 5f, 6f } };
        int[] assignments = new int[] { 0, 0, 1, 1 };
        int[] overspill = new int[0];
        CentroidSlices slices = new CentroidSlices(new int[] { 0, 2 }, new int[] { 2, 2 });

        CentroidInformation<float[]> ci = new CentroidInformation<>(dims, centroids, assignments, new SoarAssignments(overspill), slices);

        assertEquals(2, ci.numCentroids());
        assertSame(slices, ci.centroidSlices());
        // Global centroid = mean: (1+4)/2=2.5, (2+5)/2=3.5, (3+6)/2=4.5
        assertEquals(2.5f, ci.globalCentroid()[0], 1e-5f);
        assertEquals(3.5f, ci.globalCentroid()[1], 1e-5f);
        assertEquals(4.5f, ci.globalCentroid()[2], 1e-5f);
    }

    public void testOfBytesEmptyCentroids() {
        int dims = 4;
        byte[][] centroids = new byte[0][];
        int[] assignments = new int[0];
        int[] overspill = new int[0];

        CentroidInformation<byte[]> ca = CentroidInformation.ofBytes(dims, centroids, assignments, new SoarAssignments(overspill));

        assertEquals(0, ca.numCentroids());
        assertEquals(0, ca.centroids().length);
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
