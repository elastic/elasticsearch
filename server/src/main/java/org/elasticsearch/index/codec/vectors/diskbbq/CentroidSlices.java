/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

public record CentroidSlices(int[] sliceOffsets, int[] sliceNumVectors) {

    public static boolean assertSliceOffsets(int[] offsets, int numCentroids) {
        int count = offsets[0];
        for (int i = 1; i < offsets.length; i++) {
            count += offsets[i] - offsets[i - 1];
        }
        return count == numCentroids;
    }
}
