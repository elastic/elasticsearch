/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import java.util.Arrays;

/**
 * Data structure for effectively computing by how much the scale of a histogram needs to be reduced to reach a target bucket count.
 * This works by looking at each pair of neighboring buckets and checking at which scale reduction they would collapse to a single bucket.
 */
class DownscaleStats {

    // collapsedBucketCount[i] represents the number of additional
    // collapsed buckets when increasing the scale by (i+1) instead of just by (i)
    int[] collapsedBucketCount = new int[63];

    void reset() {
        Arrays.fill(collapsedBucketCount, 0);
    }

    void add(long previousBucketIndex, long currentBucketIndex) {
        if (currentBucketIndex <= previousBucketIndex) {
            throw new IllegalArgumentException("currentBucketIndex must be bigger than previousBucketIndex");
        }
        /* Below is an efficient variant of the following algorithm:
        for (int i=0; i<63; i++) {
            if (prevIndex>>(i+1) == currIndex>>(i+1)) {
                collapsedBucketCount[i]++;
                break;
            }
        }
        So we find the smallest scale reduction required to make the two buckets collapse into one
        */
        long bitXor = previousBucketIndex ^ currentBucketIndex;
        int numEqualLeadingBits = Long.numberOfLeadingZeros(bitXor);
        if (numEqualLeadingBits == 0) {
            // right-shifting will never make the buckets combine, because one is positive and the other negative
            return;
        }
        int requiredScaleChange = 64 - numEqualLeadingBits;
        collapsedBucketCount[requiredScaleChange - 1]++;
    }

    int getCollapsedBucketCountAfterScaleReduction(int reduction) {
        int totalCollapsed = 0;
        for (int i = 0; i < reduction; i++) {
            totalCollapsed += collapsedBucketCount[i];
        }
        return totalCollapsed;
    }

    int getRequiredScaleReductionToReduceBucketCountBy(int desiredCollapsedBucketCount) {
        if (desiredCollapsedBucketCount == 0) {
            return 0;
        }
        int totalCollapsed = 0;
        for (int i = 0; i < collapsedBucketCount.length; i++) {
            totalCollapsed += collapsedBucketCount[i];
            if (totalCollapsed >= desiredCollapsedBucketCount) {
                return i + 1;
            }
        }
        throw new IllegalArgumentException("it is not possible to reduce the bucket count by " + desiredCollapsedBucketCount);
    }
}
