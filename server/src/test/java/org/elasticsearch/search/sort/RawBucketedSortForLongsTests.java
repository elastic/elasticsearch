/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

public class RawBucketedSortForLongsTests extends RawBucketedSortTestCase<RawBucketedSort.ForLongs, Long> {
    @Override
    public RawBucketedSort.ForLongs build(SortOrder sortOrder, int bucketSize) {
        return new RawBucketedSort.ForLongs(bigArrays(), sortOrder, bucketSize);
    }

    @Override
    protected Long expectedSortValue(double v) {
        return (long) v;
    }

    @Override
    protected double randomValue() {
        // 2L^50 fits in the mantisa of a double which the test sort of needs.
        return randomLongBetween(-2L ^ 50, 2L ^ 50);
    }

    @Override
    protected void collect(RawBucketedSort.ForLongs sort, double value, long bucket) {
        sort.collect(bucket, (long) value);
    }
}
