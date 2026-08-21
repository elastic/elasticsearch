/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.util.IntroSorter;

/**
 * Indirect sorter that reorders an {@code int[]} index array based on double key arrays.
 * Uses Lucene's {@link IntroSorter} for O(n log n) worst-case performance.
 * <p>
 * Similar to {@link org.elasticsearch.index.codec.vectors.diskbbq.IntSorter} but supports
 * double key arrays with configurable sort direction.
 */
final class IndirectSorter {

    private IndirectSorter() {}

    /**
     * Sorts {@code indices[0..count)} in ascending order by {@code keys[indices[i]]}.
     */
    static void sortAscendingByDouble(int[] indices, double[] keys, int count) {
        new IntroSorter() {
            int pivotIdx;

            @Override
            protected void swap(int i, int j) {
                int tmp = indices[i];
                indices[i] = indices[j];
                indices[j] = tmp;
            }

            @Override
            protected int comparePivot(int j) {
                return Double.compare(keys[pivotIdx], keys[indices[j]]);
            }

            @Override
            protected void setPivot(int i) {
                pivotIdx = indices[i];
            }
        }.sort(0, count);
    }
}
