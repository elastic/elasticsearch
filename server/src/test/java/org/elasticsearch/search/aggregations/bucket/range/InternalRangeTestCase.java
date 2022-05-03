/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public abstract class InternalRangeTestCase<T extends InternalAggregation & Range> extends InternalMultiBucketAggregationTestCase<T> {

    private boolean keyed;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keyed = randomBoolean();
    }

    @Override
    protected T createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        return createTestInstance(name, metadata, aggregations, keyed);
    }

    protected abstract T createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations, boolean keyed);

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertReduced(T reduced, List<T> inputs) {
        final Map<String, Long> expectedCounts = new TreeMap<>();
        for (T input : inputs) {
            for (Range.Bucket bucket : input.getBuckets()) {
                expectedCounts.compute(
                    bucket.getKeyAsString(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount()
                );

            }
        }
        final Map<String, Long> actualCounts = new TreeMap<>();
        for (Range.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute(bucket.getKeyAsString(), (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected final void assertBucket(MultiBucketsAggregation.Bucket expected, MultiBucketsAggregation.Bucket actual, boolean checkOrder) {
        super.assertBucket(expected, actual, checkOrder);

        Class<?> internalBucketClass = internalRangeBucketClass();
        assertNotNull("Internal bucket class must not be null", internalBucketClass);
        assertTrue(internalBucketClass.isInstance(expected));

        Class<?> parsedBucketClass = parsedRangeBucketClass();
        assertNotNull("Parsed bucket class must not be null", parsedBucketClass);
        assertTrue(parsedBucketClass.isInstance(actual));

        Range.Bucket expectedRange = (Range.Bucket) expected;
        Range.Bucket actualRange = (Range.Bucket) actual;

        assertEquals(expectedRange.getFrom(), actualRange.getFrom());
        assertEquals(expectedRange.getFromAsString(), actualRange.getFromAsString());
        assertEquals(expectedRange.getTo(), actualRange.getTo());
        assertEquals(expectedRange.getToAsString(), actualRange.getToAsString());
    }

    protected abstract Class<? extends InternalMultiBucketAggregation.InternalBucket> internalRangeBucketClass();

    protected abstract Class<? extends ParsedMultiBucketAggregation.ParsedBucket> parsedRangeBucketClass();
}
