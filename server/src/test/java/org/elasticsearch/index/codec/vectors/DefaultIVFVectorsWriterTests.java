/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.index.codec.vectors.DefaultIVFVectorsWriter.mapAssignmentsByCluster;

public class DefaultIVFVectorsWriterTests extends ESTestCase {

    public void testAssignmentsByCluster() {

        // the assignments represent where vectors (by index) got assigned
        int[] assignments = new int[] { 3, 3, 2, 1, 1, 1, 1, 0, 2, 3, 4, 4, 2, 2, 2 };
        int[] soarAssignments = new int[] { 0, 0, 0, 1, 2, 3, 2, 1, 1, 1, 4, 4, 1, 2, 3 };

        assertEquals(assignments.length, soarAssignments.length);

        // 0, 1, 2, 3, 4
        // these subsequently get sorted in the order in which the centroids would have been sorted in logic
        int[] centroidOrds = new int[] { 3, 2, 4, 0, 1 };

        IntIntMap centroidOrdsToIdx = new IntIntHashMap(centroidOrds.length);
        for(int i = 0; i < centroidOrds.length; i++) {
            // idx 0 1 2 3 4 5
            // ord 3 2 0 4 1 5
            centroidOrdsToIdx.put(centroidOrds[i], i);
        }

        int[][] assignmentsByCluster = mapAssignmentsByCluster(centroidOrds.length, assignments, soarAssignments, centroidOrdsToIdx);

        assertArrayEquals(assignmentsByCluster[0], new int[] {0, 1, 5, 9, 14});
        assertArrayEquals(assignmentsByCluster[1], new int[] {2, 4, 6, 8, 12, 13, 13, 14});
        assertArrayEquals(assignmentsByCluster[2], new int[] {10, 10, 11, 11});
        assertArrayEquals(assignmentsByCluster[3], new int[] {0, 1, 2, 7});
        assertArrayEquals(assignmentsByCluster[4], new int[] {3, 3, 4, 5, 6, 7, 8, 9, 12});
    }
}
