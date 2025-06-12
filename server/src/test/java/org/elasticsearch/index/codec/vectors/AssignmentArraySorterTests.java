/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.elasticsearch.test.ESTestCase;

public class AssignmentArraySorterTests extends ESTestCase {

    public void testSortingWDuplicates() {
        int[] assignments = new int[] {1, 2, 2, 0, 1, 2, 3, 2, 3, 2};

        float[][] centroids = new float[][] {{1f, 2f, 3f}, {4f, 5f, 6f}, {7f, 8f, 9f}, {10f, 11f, 12f}};
        float[][] origCentroids = new float[centroids.length][3];
        for(int i = 0; i < centroids.length; i++) {
            System.arraycopy(centroids[i], 0, origCentroids[i], 0, 3);
        }
        int[] centroidOrds = new int[centroids.length];
        for(int i = 0; i < centroids.length; i++) {
            centroidOrds[i] = i;
        }
        int[] ordering = new int[] {0, 2, 2, 1};

        AssignmentArraySorter sorter = new AssignmentArraySorter(centroids, centroidOrds, ordering);
        sorter.sort(0, centroids.length);

        assertEquals(0, centroidOrds[0]);
        assertEquals(3, centroidOrds[1]);
        assertEquals(1, centroidOrds[2]);
        assertEquals(2, centroidOrds[3]);

        assertArrayEquals(new float[]{1f, 2f, 3f}, centroids[0], 0.1f);
        assertArrayEquals(new float[]{10f, 11f, 12f}, centroids[1], 0.1f);
        assertArrayEquals(new float[]{4f, 5f, 6f}, centroids[2], 0.1f);
        assertArrayEquals(new float[]{7f, 8f, 9f}, centroids[3], 0.1f);

        assertArrayEquals(origCentroids[assignments[0]], centroids[centroidOrds[3]], 0.1f);
        assertArrayEquals(origCentroids[assignments[1]], centroids[centroidOrds[1]], 0.1f);
        assertArrayEquals(origCentroids[assignments[2]], centroids[centroidOrds[1]], 0.1f);
        assertArrayEquals(origCentroids[assignments[3]], centroids[centroidOrds[0]], 0.1f);
        assertArrayEquals(origCentroids[assignments[4]], centroids[centroidOrds[3]], 0.1f);
        assertArrayEquals(origCentroids[assignments[5]], centroids[centroidOrds[1]], 0.1f);
        assertArrayEquals(origCentroids[assignments[6]], centroids[centroidOrds[2]], 0.1f);
        assertArrayEquals(origCentroids[assignments[7]], centroids[centroidOrds[1]], 0.1f);
        assertArrayEquals(origCentroids[assignments[8]], centroids[centroidOrds[2]], 0.1f);
        assertArrayEquals(origCentroids[assignments[9]], centroids[centroidOrds[1]], 0.1f);
    }
}
