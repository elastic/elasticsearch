/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.TestBlockFactory;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LongBucketedSortTests extends ESTestCase {
    /**
     * Build a {@link LongBucketedSort} to test. Sorts built by this method shouldn't need scores.
     */
    private LongBucketedSort build(SortOrder sortOrder, int bucketSize) {
        return new LongBucketedSort(bigArrays(), sortOrder, bucketSize);
    }

    /**
     * Build the expected correctly typed value for a value.
     */
    private Long expectedSortValue(double v) {
        return (long) v;
    }

    /**
     * A random value for testing, with the appropriate precision for the type we're testing.
     */
    private double randomValue() {
        // 2L^50 fits in the mantisa of a double which the test sort of needs.
        return randomLongBetween(-2L ^ 50, 2L ^ 50);
    }

    /**
     * Collect a value into the sort.
     * @param value value to collect, always sent as double just to have
     *        a number to test. Subclasses should cast to their favorite types
     */
    private void collect(LongBucketedSort sort, double value, long bucket) {
        sort.collect((long) value, bucket);
    }

    public final void testNeverCalled() {
        SortOrder order = randomFrom(SortOrder.values());
        try (LongBucketedSort sort = build(order, 1)) {
            assertThat(sort.getOrder(), equalTo(order));
            assertBlock(sort, randomNonNegativeInt());
        }
    }

    public final void testSingleDoc() {
        try (LongBucketedSort sort = build(randomFrom(SortOrder.values()), 1)) {
            collect(sort, 1, 0);

            assertBlock(sort, 0, expectedSortValue(1));
        }
    }

    public void testNonCompetitive() {
        try (LongBucketedSort sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 0);
            collect(sort, 1, 0);

            assertBlock(sort, 0, expectedSortValue(2));
        }
    }

    public void testCompetitive() {
        try (LongBucketedSort sort = build(SortOrder.DESC, 1)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);

            assertBlock(sort, 0, expectedSortValue(2));
        }
    }

    public void testNegativeValue() {
        try (LongBucketedSort sort = build(SortOrder.DESC, 1)) {
            collect(sort, -1, 0);
            assertBlock(sort, 0, expectedSortValue(-1));
        }
    }

    public void testSomeBuckets() {
        try (LongBucketedSort sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 0);
            collect(sort, 2, 1);
            collect(sort, 2, 2);
            collect(sort, 3, 0);

            assertBlock(sort, 0, expectedSortValue(3));
            assertBlock(sort, 1, expectedSortValue(2));
            assertBlock(sort, 2, expectedSortValue(2));
            assertBlock(sort, 3);
        }
    }

    public void testBucketGaps() {
        try (LongBucketedSort sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 0);
            collect(sort, 2, 2);

            assertBlock(sort, 0, expectedSortValue(2));
            assertBlock(sort, 1);
            assertBlock(sort, 2, expectedSortValue(2));
            assertBlock(sort, 3);
        }
    }

    public void testBucketsOutOfOrder() {
        try (LongBucketedSort sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 1);
            collect(sort, 2, 0);

            assertBlock(sort, 0, expectedSortValue(2.0));
            assertBlock(sort, 1, expectedSortValue(2.0));
            assertBlock(sort, 2);
        }
    }

    public void testManyBuckets() {
        // Collect the buckets in random order
        int[] buckets = new int[10000];
        for (int b = 0; b < buckets.length; b++) {
            buckets[b] = b;
        }
        Collections.shuffle(Arrays.asList(buckets), random());

        double[] maxes = new double[buckets.length];

        try (LongBucketedSort sort = build(SortOrder.DESC, 1)) {
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
                assertBlock(sort, b, expectedSortValue(maxes[b]));
            }
            assertBlock(sort, buckets.length);
        }
    }

    public void testTwoHitsDesc() {
        try (LongBucketedSort sort = build(SortOrder.DESC, 2)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            assertBlock(sort, 0, expectedSortValue(3), expectedSortValue(2));
        }
    }

    public void testTwoHitsAsc() {
        try (LongBucketedSort sort = build(SortOrder.ASC, 2)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            assertBlock(sort, 0, expectedSortValue(1), expectedSortValue(2));
        }
    }

    public void testTwoHitsTwoBucket() {
        try (LongBucketedSort sort = build(SortOrder.DESC, 2)) {
            collect(sort, 1, 0);
            collect(sort, 1, 1);
            collect(sort, 2, 0);
            collect(sort, 2, 1);
            collect(sort, 3, 0);
            collect(sort, 3, 1);
            collect(sort, 4, 1);

            assertBlock(sort, 0, expectedSortValue(3), expectedSortValue(2));
            assertBlock(sort, 1, expectedSortValue(4), expectedSortValue(3));
        }
    }

    public void testManyBucketsManyHits() {
        // Set the values in random order
        double[] values = new double[10000];
        for (int v = 0; v < values.length; v++) {
            values[v] = randomValue();
        }
        Collections.shuffle(Arrays.asList(values), random());

        int buckets = between(2, 100);
        int bucketSize = between(2, 100);
        try (LongBucketedSort sort = build(SortOrder.DESC, bucketSize)) {
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
                assertBlock(
                    sort,
                    bucket,
                    bucketValues.stream()
                        .sorted((lhs, rhs) -> rhs.compareTo(lhs))
                        .limit(bucketSize)
                        .mapToLong(this::expectedSortValue)
                        .toArray()
                );
            }
            assertBlock(sort, buckets);
        }
    }

    public void testMergeWithHeap() {
        try (LongBucketedSort sort = build(SortOrder.ASC, 3)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            try (LongBucketedSort other = build(SortOrder.ASC, 3)) {
                collect(other, 1, 0);
                collect(other, 2, 0);
                collect(other, 3, 0);

                sort.merge(0, other, 0);
            }

            assertBlock(sort, 0, expectedSortValue(1), expectedSortValue(1), expectedSortValue(2));
        }
    }

    public void testMergeNotFull() {
        try (LongBucketedSort sort = build(SortOrder.ASC, 3)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);

            try (LongBucketedSort other = build(SortOrder.ASC, 3)) {
                collect(other, 1, 0);
                collect(other, 2, 0);

                sort.merge(0, other, 0);
            }

            assertBlock(sort, 0, expectedSortValue(1), expectedSortValue(1), expectedSortValue(2));
        }
    }

    public void testMergeEmptyReceiver() {
        try (LongBucketedSort sort = build(SortOrder.ASC, 3)) {
            try (LongBucketedSort other = build(SortOrder.ASC, 3)) {
                collect(other, 1, 0);
                collect(other, 2, 0);
                collect(other, 3, 0);

                sort.merge(0, other, 0);
            }

            assertBlock(sort, 0, expectedSortValue(1), expectedSortValue(2), expectedSortValue(3));
        }
    }

    public void testMergeEmptyEmitter() {
        try (LongBucketedSort sort = build(SortOrder.ASC, 3)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            try (LongBucketedSort other = build(SortOrder.ASC, 3)) {
                sort.merge(0, other, 0);
            }

            assertBlock(sort, 0, expectedSortValue(1), expectedSortValue(2), expectedSortValue(3));
        }
    }

    private BigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private void assertBlock(LongBucketedSort sort, int groupId, long... values) {
        var blockFactory = TestBlockFactory.getNonBreakingInstance();

        try (var intVector = blockFactory.newConstantIntVector(groupId, 1)) {
            var block = sort.toBlock(blockFactory, intVector);

            assertThat(block.getPositionCount(), equalTo(1));
            assertThat(block.getTotalValueCount(), equalTo(values.length));

            if (values.length == 0) {
                assertThat(block.isNull(0), equalTo(true));
            } else {
                assertThat(block.elementType(), equalTo(ElementType.LONG));
                var longBlock = (LongBlock) block;
                for (int i = 0; i < values.length; i++) {
                    assertThat(longBlock.getLong(i), equalTo(values[i]));
                }
            }
        }
    }
}
