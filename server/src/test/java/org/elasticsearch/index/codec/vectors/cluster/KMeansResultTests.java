/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

public class KMeansResultTests extends ESTestCase {

    public void testMergeOffsetsAssignmentsAndSoarAssignments() {
        int dims = randomIntBetween(1, 4);
        int centroidCount1 = randomIntBetween(1, 4);
        int centroidCount2 = randomIntBetween(1, 4);
        int assignmentsSize1 = randomIntBetween(1, 8);
        int assignmentsSize2 = randomIntBetween(1, 8);

        float[][] centroids1 = randomCentroids(centroidCount1, dims);
        float[][] centroids2 = randomCentroids(centroidCount2, dims);
        int[] assignments1 = randomAssignments(assignmentsSize1, centroidCount1);
        int[] assignments2 = randomAssignments(assignmentsSize2, centroidCount2);
        int[] soarAssignments1 = randomSoarAssignments(assignmentsSize1, centroidCount1, false);
        int[] soarAssignments2 = randomSoarAssignments(assignmentsSize2, centroidCount2, true);

        int[] expectedAssignments = new int[assignmentsSize1 + assignmentsSize2];
        System.arraycopy(assignments1, 0, expectedAssignments, 0, assignmentsSize1);
        for (int i = 0; i < assignmentsSize2; i++) {
            expectedAssignments[assignmentsSize1 + i] = assignments2[i] + centroidCount1;
        }

        int[] expectedSoarAssignments = new int[assignmentsSize1 + assignmentsSize2];
        System.arraycopy(soarAssignments1, 0, expectedSoarAssignments, 0, assignmentsSize1);
        for (int i = 0; i < assignmentsSize2; i++) {
            int soar = soarAssignments2[i];
            expectedSoarAssignments[assignmentsSize1 + i] = soar == NO_SOAR_ASSIGNMENT ? NO_SOAR_ASSIGNMENT : soar + centroidCount1;
        }

        float[][] expectedCentroids = new float[centroidCount1 + centroidCount2][];
        System.arraycopy(centroids1, 0, expectedCentroids, 0, centroidCount1);
        System.arraycopy(centroids2, 0, expectedCentroids, centroidCount1, centroidCount2);

        KMeansResult merged = KMeansResult.merge(
            List.of(
                new KMeansResult(centroids1, assignments1, soarAssignments1),
                new KMeansResult(centroids2, assignments2, soarAssignments2)
            )
        );

        assertCentroidsEqual(expectedCentroids, merged.centroids());
        assertArrayEquals(expectedAssignments, merged.assignments());
        assertArrayEquals(expectedSoarAssignments, merged.soarAssignments());
    }

    public void testMergeFillsMissingSoarAssignments() {
        int dims = randomIntBetween(1, 4);
        int centroidCount1 = randomIntBetween(1, 4);
        int centroidCount2 = randomIntBetween(1, 4);
        int assignmentsSize1 = randomIntBetween(1, 8);
        int assignmentsSize2 = randomIntBetween(1, 8);

        float[][] centroids1 = randomCentroids(centroidCount1, dims);
        float[][] centroids2 = randomCentroids(centroidCount2, dims);
        int[] assignments1 = randomAssignments(assignmentsSize1, centroidCount1);
        int[] assignments2 = randomAssignments(assignmentsSize2, centroidCount2);
        int[] soarAssignments1 = randomSoarAssignments(assignmentsSize1, centroidCount1, false);

        int[] expectedAssignments = new int[assignmentsSize1 + assignmentsSize2];
        System.arraycopy(assignments1, 0, expectedAssignments, 0, assignmentsSize1);
        for (int i = 0; i < assignmentsSize2; i++) {
            expectedAssignments[assignmentsSize1 + i] = assignments2[i] + centroidCount1;
        }

        int[] expectedSoarAssignments = new int[assignmentsSize1 + assignmentsSize2];
        System.arraycopy(soarAssignments1, 0, expectedSoarAssignments, 0, assignmentsSize1);
        for (int i = assignmentsSize1; i < expectedSoarAssignments.length; i++) {
            expectedSoarAssignments[i] = NO_SOAR_ASSIGNMENT;
        }

        float[][] expectedCentroids = new float[centroidCount1 + centroidCount2][];
        System.arraycopy(centroids1, 0, expectedCentroids, 0, centroidCount1);
        System.arraycopy(centroids2, 0, expectedCentroids, centroidCount1, centroidCount2);

        KMeansResult merged = KMeansResult.merge(
            List.of(new KMeansResult(centroids1, assignments1, soarAssignments1), new KMeansResult(centroids2, assignments2, new int[0]))
        );

        assertCentroidsEqual(expectedCentroids, merged.centroids());
        assertArrayEquals(expectedAssignments, merged.assignments());
        assertArrayEquals(expectedSoarAssignments, merged.soarAssignments());
    }

    private static void assertCentroidsEqual(float[][] expected, float[][] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(expected[i], actual[i], 0f);
        }
    }

    private float[][] randomCentroids(int count, int dims) {
        float[][] centroids = new float[count][dims];
        for (int i = 0; i < count; i++) {
            for (int d = 0; d < dims; d++) {
                centroids[i][d] = random().nextFloat();
            }
        }
        return centroids;
    }

    private int[] randomAssignments(int count, int centroidCount) {
        int[] assignments = new int[count];
        for (int i = 0; i < count; i++) {
            assignments[i] = randomIntBetween(0, centroidCount - 1);
        }
        return assignments;
    }

    private int[] randomSoarAssignments(int count, int centroidCount, boolean ensureAssigned) {
        int[] assignments = new int[count];
        for (int i = 0; i < count; i++) {
            assignments[i] = randomBoolean() ? NO_SOAR_ASSIGNMENT : randomIntBetween(0, centroidCount - 1);
        }
        if (ensureAssigned && count > 0) {
            assignments[randomIntBetween(0, count - 1)] = randomIntBetween(0, centroidCount - 1);
        }
        return assignments;
    }
}
