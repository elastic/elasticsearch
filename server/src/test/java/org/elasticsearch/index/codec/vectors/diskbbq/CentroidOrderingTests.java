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

    public void testReorderingImprovesNeighborCostOnClusteredData() throws IOException {
        int dim = 2;
        int groups = 4;
        int groupSize = 32;
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

        int[] assignments = new int[128];
        for (int i = 0; i < assignments.length; i++) {
            assignments[i] = i % centroids.length;
        }

        int[] overspill = new int[0];
        int trials = 5;
        int improved = 0;
        for (int i = 0; i < trials; i++) {
            float[][] shuffled = new float[centroids.length][dim];
            int[] order = new int[centroids.length];
            for (int j = 0; j < order.length; j++) {
                order[j] = j;
            }
            for (int j = order.length - 1; j > 0; j--) {
                int swapWith = randomIntBetween(0, j);
                int tmp = order[j];
                order[j] = order[swapWith];
                order[swapWith] = tmp;
            }
            for (int j = 0; j < centroids.length; j++) {
                shuffled[j] = centroids[order[j]];
            }
            float baselineCost = neighborCost(shuffled);
            CentroidOrdering.Result result = CentroidOrdering.reorder(dim, shuffled, assignments, overspill, null);
            float reorderedCost = neighborCost(result.centroids());
            if (reorderedCost < baselineCost) {
                improved++;
            }
        }
        assertTrue(improved >= 3);
    }

    private static float neighborCost(float[][] centroids) {
        int n = centroids.length;
        float cost = 0.0f;
        for (int i = 0; i < n; i++) {
            int next = Math.min(i + 1, n - 1);
            float dx = centroids[i][0] - centroids[next][0];
            float dy = centroids[i][1] - centroids[next][1];
            cost += dx * dx + dy * dy;
        }
        return cost;
    }
}
