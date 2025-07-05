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

import java.util.Arrays;

import static org.elasticsearch.index.codec.vectors.DefaultIVFVectorsWriter.mapAssignmentsByCluster;

public class DefaultIVFVectorsWriterTests extends ESTestCase {

    // FIXME: clean this up
    public void testAssignmentsByCluster() {

        // the assignments represent where vectors (by index) got assigned
        int[] assignments = new int[] { 3, 3, 2, 1, 1, 1, 1, 0, 2, 3, 4, 4, 2, 2, 2 };
        int[] soarAssignments = new int[] { 0, 0, 0, 1, 2, 3, 2, 1, 1, 1, 4, 4, 1, 2, 3 };

        assert assignments.length == soarAssignments.length;

        // 0, 1, 2, 3, 4
        // these subsequently get sorted in the order in which the centroids would have been sorted in logic
        int[] centroidOrds = new int[] { 3, 2, 4, 0, 1 };

        int[][] assignmentsByCluster = mapAssignmentsByCluster(centroidOrds.length, assignments, soarAssignments, centroidOrds);

        /*
         * correct answer
         * [0, 1, 9, 5, 14]
         * [2, 8, 12, 13, 14, 4, 6, 13]
         * [10, 11, 10, 11]
         * [7, 0, 1, 2]
         * [3, 4, 5, 6, 3, 7, 8, 9, 12]
         */
        for (int i = 0; i < assignmentsByCluster.length; i++) {
            System.out.println(Arrays.toString(assignmentsByCluster[i]));
        }

        /*
         * [0, 1, 5, 9, 14]
         * [2, 4, 6, 8, 12, 13, 13, 14]
         * [10, 10, 11, 11]
         * [0, 1, 2, 7]
         * [3, 3, 4, 5, 6, 7, 8, 9, 12]
         */
    }
}
