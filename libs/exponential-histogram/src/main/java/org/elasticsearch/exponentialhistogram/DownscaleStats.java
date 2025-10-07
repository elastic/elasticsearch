/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX_BITS;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;

/**
 * A data structure for efficiently computing the required scale reduction for a histogram to reach a target number of buckets.
 * This works by examining pairs of neighboring buckets and determining at which scale reduction they would merge into a single bucket.
 */
class DownscaleStats {

    static final long SIZE = RamUsageEstimator.shallowSizeOf(DownscaleStats.class) + RamEstimationUtil.estimateIntArray(MAX_INDEX_BITS);

    // collapsedBucketCount[i] stores the number of additional
    // collapsed buckets when increasing the scale by (i+1) instead of just by (i)
    int[] collapsedBucketCount = new int[MAX_INDEX_BITS];

    /**
     * Resets the data structure to its initial state.
     */
    void reset() {
        Arrays.fill(collapsedBucketCount, 0);
    }

    /**
     * Adds a pair of neighboring bucket indices to track for potential merging.
     *
     * @param previousBucketIndex the index of the previous bucket
     * @param currentBucketIndex the index of the current bucket
     */
    void add(long previousBucketIndex, long currentBucketIndex) {
        assert currentBucketIndex > previousBucketIndex;
        assert previousBucketIndex >= MIN_INDEX && previousBucketIndex <= MAX_INDEX;
        assert currentBucketIndex <= MAX_INDEX;
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
        // if there are zero equal leading bits, the indices have a different sign.
        // Therefore right-shifting will never make the buckets combine
        if (numEqualLeadingBits > 0) {
            int requiredScaleChange = 64 - numEqualLeadingBits;
            collapsedBucketCount[requiredScaleChange - 1]++;
        }
    }

    /**
     * Returns the number of buckets that will be merged after applying the given scale reduction.
     *
     * @param reduction the scale reduction factor
     * @return the number of buckets that will be merged
     */
    int getCollapsedBucketCountAfterScaleReduction(int reduction) {
        assert reduction >= 0 && reduction <= MAX_INDEX_BITS;
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
        assert desiredCollapsedBucketCount >= 0;
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
        throw new IllegalStateException("Cannot reduce the bucket count by " + desiredCollapsedBucketCount);
    }
}
