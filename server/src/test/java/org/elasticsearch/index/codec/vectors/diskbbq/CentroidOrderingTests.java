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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class CentroidOrderingTests extends ESTestCase {

    public void testNoOpWhenSmall() throws IOException {
        float[][] centroids = new float[4][2];
        for (int i = 0; i < centroids.length; i++) {
            centroids[i][0] = i;
            centroids[i][1] = i * 2f;
        }
        int[] assignments = new int[] { 0, 1, 2, 3, 1, 0 };
        int[] overspill = new int[] { -1, 2, 3, -1 };

        CentroidOrdering.Result result = CentroidOrdering.reorder(2, centroids, assignments, overspill, null);

        assertSame(centroids, result.centroids());
        assertSame(assignments, result.assignments());
        assertSame(overspill, result.overspillAssignments());
    }

    public void testAssignmentsRemappedAndNegativesPreserved() throws IOException {
        int dim = 2;
        int n = 32;
        float[][] centroids = new float[n][dim];
        for (int i = 0; i < n; i++) {
            centroids[i][0] = i;
            centroids[i][1] = i * 0.5f;
        }
        int[] assignments = new int[64];
        int[] overspill = new int[64];
        for (int i = 0; i < assignments.length; i++) {
            assignments[i] = i % n;
            overspill[i] = (i % 3 == 0) ? -1 : (n - 1 - (i % n));
        }

        CentroidOrdering.Result result = CentroidOrdering.reorder(dim, centroids, assignments, overspill, null);
        int[] newAssignments = result.assignments();
        int[] newOverspill = result.overspillAssignments();

        assertEquals(assignments.length, newAssignments.length);
        assertEquals(overspill.length, newOverspill.length);
        int[] mappedCounts = new int[n];
        for (int value : newAssignments) {
            assertTrue(value >= 0);
            assertTrue(value < n);
            mappedCounts[value]++;
        }
        int total = Arrays.stream(mappedCounts).sum();
        assertEquals(assignments.length, total);
        for (int i = 0; i < overspill.length; i++) {
            if (overspill[i] < 0) {
                assertEquals(-1, newOverspill[i]);
            } else {
                assertTrue(newOverspill[i] >= 0);
                assertTrue(newOverspill[i] < n);
            }
        }
    }

    public void testReorderingImprovesSlidingWindowProximityOnClusteredData() throws IOException {
        int dim = 2;
        int groups = 4;
        int groupSize = 32;
        int centroidCount = groups * groupSize;
        int[] assignments = new int[centroidCount];
        for (int i = 0; i < assignments.length; i++) {
            assignments[i] = i;
        }
        int[] overspill = new int[0];
        int windowRadius = 10;
        int trials = 8;

        double totalBaselineMismatch = 0.0d;
        double totalReorderedMismatch = 0.0d;
        for (int i = 0; i < trials; i++) {
            float[][] shuffled = shuffledCopy(buildInterleavedClusteredCentroids(groups, groupSize, dim));
            double baselineMismatch = slidingWindowGroupMismatchRate(shuffled, windowRadius, groupSize);
            CentroidOrdering.Result result = CentroidOrdering.reorder(dim, shuffled, assignments, overspill, null);
            double reorderedMismatch = slidingWindowGroupMismatchRate(result.centroids(), windowRadius, groupSize);
            assertTrue(
                "Expected every trial to improve sliding-window locality, baseline="
                    + baselineMismatch
                    + ", reordered="
                    + reorderedMismatch,
                reorderedMismatch < baselineMismatch
            );
            totalBaselineMismatch += baselineMismatch;
            totalReorderedMismatch += reorderedMismatch;
        }

        double meanBaselineMismatch = totalBaselineMismatch / trials;
        double meanReorderedMismatch = totalReorderedMismatch / trials;
        assertTrue(
            "Expected strong mismatch reduction, baseline=" + meanBaselineMismatch + ", reordered=" + meanReorderedMismatch,
            meanReorderedMismatch <= meanBaselineMismatch * 0.35d
        );
    }

    public void testSymmetrizeSkipsReplacingStrongCandidate() throws Exception {
        int[][] neighbors = new int[][] { { 1, 2 }, { 2, 2 }, { 1, 0 } };
        float[][] distances = new float[][] { { 6.0f, 7.0f }, { 4.0f, 5.0f }, { 1.0f, 1.0f } };

        invokeSymmetrizeNeighbors(neighbors, distances, 2);

        assertArrayEquals(new int[] { 2, 2 }, neighbors[1]);
        assertArrayEquals(new float[] { 4.0f, 5.0f }, distances[1], 0.0f);
    }

    public void testSymmetrizeReplacementCandidateUsesOnlyTopKWindow() throws Exception {
        int[][] neighbors = new int[][] { { 1, 2 }, { 2, 2, 2, 2 }, { 0, 1 } };
        float[][] distances = new float[][] { { 0.1f, 9.0f }, { 0.2f, 0.8f, 100.0f, 200.0f }, { 1.0f, 1.0f } };

        invokeSymmetrizeNeighbors(neighbors, distances, 2);

        assertEquals(2, neighbors[1][0]);
        assertEquals(0, neighbors[1][1]);
        assertEquals(0.2f, distances[1][0], 0.0f);
        assertEquals(0.1f, distances[1][1], 0.0f);
    }

    private float[][] buildInterleavedClusteredCentroids(int groups, int groupSize, int dim) {
        float[][] centroids = new float[groups * groupSize][dim];
        int idx = 0;
        for (int i = 0; i < groupSize; i++) {
            for (int g = 0; g < groups; g++) {
                float baseX = g * 100f;
                float baseY = g * 100f;
                centroids[idx][0] = baseX + (i % 4) * 0.1f;
                centroids[idx][1] = baseY + (i / 4) * 0.1f;
                idx++;
            }
        }
        return centroids;
    }

    private float[][] shuffledCopy(float[][] centroids) {
        int[] order = new int[centroids.length];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        for (int i = order.length - 1; i > 0; i--) {
            int swapWith = randomIntBetween(0, i);
            int tmp = order[i];
            order[i] = order[swapWith];
            order[swapWith] = tmp;
        }
        float[][] shuffled = new float[centroids.length][];
        for (int i = 0; i < centroids.length; i++) {
            shuffled[i] = Arrays.copyOf(centroids[order[i]], centroids[order[i]].length);
        }
        return shuffled;
    }

    private static double slidingWindowGroupMismatchRate(float[][] centroids, int windowRadius, int groupSize) {
        long pairs = 0L;
        long mismatches = 0L;
        for (int i = 0; i < centroids.length; i++) {
            int group = (int) (centroids[i][0] / 100f);
            int start = Math.max(0, i - windowRadius);
            int end = Math.min(centroids.length - 1, i + windowRadius);
            for (int j = start; j <= end; j++) {
                if (j == i) {
                    continue;
                }
                pairs++;
                int otherGroup = (int) (centroids[j][0] / 100f);
                if (group != otherGroup) {
                    mismatches++;
                }
            }
        }
        assertTrue(pairs > 0L);
        assertTrue(groupSize > windowRadius);
        return (double) mismatches / pairs;
    }

    private static void invokeSymmetrizeNeighbors(int[][] neighbors, float[][] distances, int k) throws Exception {
        Method method = CentroidOrdering.class.getDeclaredMethod("symmetrizeNeighbors", int[][].class, float[][].class, int.class);
        method.setAccessible(true);
        method.invoke(null, neighbors, distances, k);
    }
}
