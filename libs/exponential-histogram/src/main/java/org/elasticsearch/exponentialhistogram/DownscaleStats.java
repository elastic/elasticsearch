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
 * A data structure for efficiently computing the required scale reduction for a histogram to reach a target number of buckets.
 * This works by examining pairs of neighboring buckets and determining at which scale reduction they would merge into a single bucket.
 */
class DownscaleStats {

    // collapsedBucketCount[i] stores the number of additional
    // collapsed buckets when increasing the scale by (i+1) instead of just by (i)
    int[] collapsedBucketCount = new int[63];

    /**
     * Resets the data structure to its initial state.
     */
    void reset() {
        Arrays.fill(collapsedBucketCount, 0);
    }

    void add(long previousBucketIndex, long currentBucketIndex) {
        if (currentBucketIndex <= previousBucketIndex) {
            throw new IllegalArgumentException("currentBucketIndex must be greater than previousBucketIndex");
        }
        /*
         * Below is an efficient variant of the following algorithm:
         * for (int i=0; i<63; i++) {
         *     if (prevIndex>>(i+1) == currIndex>>(i+1)) {
         *         collapsedBucketCount[i]++;
         *         break;
         *     }
         * }
         * So we find the smallest scale reduction required to make the two buckets collapse into one.
         */
        long bitXor = previousBucketIndex ^ currentBucketIndex;
        int numEqualLeadingBits = Long.numberOfLeadingZeros(bitXor);
        if (numEqualLeadingBits == 0) {
            // right-shifting will never make the buckets combine, because one is positive and the other is negative
            return;
        }
        int requiredScaleChange = 64 - numEqualLeadingBits;
        collapsedBucketCount[requiredScaleChange - 1]++;
    }

    /**
     * Returns the number of buckets that will be merged after applying the given scale reduction.
     *
     * @param reduction the scale reduction factor
     * @return the number of buckets that will be merged
     */
    int getCollapsedBucketCountAfterScaleReduction(int reduction) {
        int totalCollapsed = 0;
        for (int i = 0; i < reduction; i++) {
            totalCollapsed += collapsedBucketCount[i];
        }
        return totalCollapsed;
    }

    /**
     * Returns the required scale reduction to reduce the number of buckets by at least the given amount.
     *
     * @param desiredCollapsedBucketCount the target number of buckets to collapse
     * @return the required scale reduction
     */
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
        throw new IllegalArgumentException("Cannot reduce the bucket count by " + desiredCollapsedBucketCount);
    }
}
