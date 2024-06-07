/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public abstract class RawBucketedSortTestCase<T extends RawBucketedSort<V>, V> extends ESTestCase {
    /**
     * Build a {@link RawBucketedSort} to test. Sorts built by this method shouldn't need scores.
     */
    protected abstract T build(SortOrder sortOrder, int bucketSize);

    /**
     * Build the expected correctly typed value for a value.
     */
    protected abstract V expectedSortValue(double v);

    /**
     * A random value for testing, with the appropriate precision for the type we're testing.
     */
    protected abstract double randomValue();

    /**
     * Collect a value into the sort.
     * @param value value to collect, always sent as double just to have
     *        a number to test. Subclasses should cast to their favorite types
     */
    protected abstract void collect(T sort, double value, long bucket);

    public final void testNeverCalled() throws IOException {
        SortOrder order = randomFrom(SortOrder.values());
        try (T sort = build(order, 1)) {
            assertThat(sort.getOrder(), equalTo(order));
            assertThat(sort.getValues(randomNonNegativeLong()), empty());
        }
    }

    public final void testSingleDoc() throws IOException {
        try (T sort = build(randomFrom(SortOrder.values()), 1)) {
            collect(sort, 1, 0);

            assertThat(sort.getValues(0), contains(expectedSortValue(1)));
        }
    }

    public void testNonCompetitive() throws IOException {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 0);
            collect(sort, 1, 0);

            assertThat(sort.getValues(0), contains(expectedSortValue(2)));
        }
    }

    public void testCompetitive() throws IOException {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);

            assertThat(sort.getValues(0), contains(expectedSortValue(2)));
        }
    }

    public void testNegativeValue() throws IOException {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, -1, 0);
            assertThat(sort.getValues(0), contains(expectedSortValue(-1)));
        }
    }

    public void testSomeBuckets() throws IOException {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 0);
            collect(sort, 2, 1);
            collect(sort, 2, 2);
            collect(sort, 3, 0);

            assertThat(sort.getValues(0), contains(expectedSortValue(3)));
            assertThat(sort.getValues(1), contains(expectedSortValue(2)));
            assertThat(sort.getValues(2), contains(expectedSortValue(2)));
            assertThat(sort.getValues(3), empty());
        }
    }

    public void testBucketGaps() throws IOException {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 0);
            collect(sort, 2, 2);

            assertThat(sort.getValues(0), contains(expectedSortValue(2)));
            assertThat(sort.getValues(1), empty());
            assertThat(sort.getValues(2), contains(expectedSortValue(2)));
            assertThat(sort.getValues(3), empty());
        }
    }

    public void testBucketsOutOfOrder() throws IOException {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 1);
            collect(sort, 2, 0);

            assertThat(sort.getValues(0), contains(expectedSortValue(2.0)));
            assertThat(sort.getValues(1), contains(expectedSortValue(2.0)));
            assertThat(sort.getValues(2), empty());
        }
    }

    public void testManyBuckets() throws IOException {
        // Collect the buckets in random order
        int[] buckets = new int[10000];
        for (int b = 0; b < buckets.length; b++) {
            buckets[b] = b;
        }
        Collections.shuffle(Arrays.asList(buckets), random());

        double[] maxes = new double[buckets.length];

        try (T sort = build(SortOrder.DESC, 1)) {
            for (int b : buckets) {
                maxes[b] = 2;
                collect(sort, 2, b);
                if (randomBoolean()) {
                    maxes[b] = 3;
                    collect(sort, 3, b);
                }
                if (randomBoolean()) {
                    collect(sort, -1, b);
                }
            }
            for (int b = 0; b < buckets.length; b++) {
                assertThat(sort.getValues(b), contains(expectedSortValue(maxes[b])));
            }
            assertThat(sort.getValues(buckets.length), empty());
        }
    }

    public void testTwoHitsDesc() throws IOException {
        try (T sort = build(SortOrder.DESC, 2)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            assertThat(sort.getValues(0), contains(expectedSortValue(3), expectedSortValue(2)));
        }
    }

    public void testTwoHitsAsc() throws IOException {
        try (T sort = build(SortOrder.ASC, 2)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            assertThat(sort.getValues(0), contains(expectedSortValue(1), expectedSortValue(2)));
        }
    }

    public void testTwoHitsTwoBucket() throws IOException {
        try (T sort = build(SortOrder.DESC, 2)) {
            collect(sort, 1, 0);
            collect(sort, 1, 1);
            collect(sort, 2, 0);
            collect(sort, 2, 1);
            collect(sort, 3, 0);
            collect(sort, 3, 1);
            collect(sort, 4, 1);

            assertThat(sort.getValues(0), contains(expectedSortValue(3), expectedSortValue(2)));
            assertThat(sort.getValues(1), contains(expectedSortValue(4), expectedSortValue(3)));
        }
    }

    public void testManyBucketsManyHits() throws IOException {
        // Set the values in random order
        double[] values = new double[10000];
        for (int v = 0; v < values.length; v++) {
            values[v] = randomValue();
        }
        Collections.shuffle(Arrays.asList(values), random());

        int buckets = between(2, 100);
        int bucketSize = between(2, 100);
        try (T sort = build(SortOrder.DESC, bucketSize)) {
            BitArray[] bucketUsed = new BitArray[buckets];
            Arrays.setAll(bucketUsed, i -> new BitArray(values.length, bigArrays()));
            for (int doc = 0; doc < values.length; doc++) {
                for (int bucket = 0; bucket < buckets; bucket++) {
                    if (randomBoolean()) {
                        bucketUsed[bucket].set(doc);
                        collect(sort, values[doc], bucket);
                    }
                }
            }
            for (int bucket = 0; bucket < buckets; bucket++) {
                List<Double> bucketValues = new ArrayList<>(values.length);
                for (int doc = 0; doc < values.length; doc++) {
                    if (bucketUsed[bucket].get(doc)) {
                        bucketValues.add(values[doc]);
                    }
                }
                bucketUsed[bucket].close();
                assertThat(
                    "Bucket " + bucket,
                    sort.getValues(bucket),
                    contains(
                        bucketValues.stream()
                            .sorted((lhs, rhs) -> rhs.compareTo(lhs))
                            .limit(bucketSize)
                            .map(s -> equalTo(expectedSortValue(s)))
                            .collect(toList())
                    )
                );
            }
            assertThat(sort.getValues(buckets), empty());
        }
    }

    protected BigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }
}
