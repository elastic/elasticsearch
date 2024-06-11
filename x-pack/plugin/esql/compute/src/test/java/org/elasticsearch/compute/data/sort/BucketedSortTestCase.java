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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.TestBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public abstract class BucketedSortTestCase<T extends Releasable> extends ESTestCase {
    /**
     * Build a {@link T} to test. Sorts built by this method shouldn't need scores.
     */
    protected abstract T build(SortOrder sortOrder, int bucketSize);

    /**
     * Build the expected correctly typed value for a value.
     */
    protected abstract Object expectedValue(double v);

    /**
     * A random value for testing, with the appropriate precision for the type we're testing.
     */
    protected abstract double randomValue();

    /**
     * Collect a value into the sort.
     * @param value value to collect, always sent as double just to have
     *        a number to test. Subclasses should cast to their favorite types
     */
    protected abstract void collect(T sort, double value, int bucket);

    protected abstract void merge(T sort, int groupId, T other, int otherGroupId);

    protected abstract Block toBlock(T sort, BlockFactory blockFactory, IntVector selected);

    protected abstract void assertBlockTypeAndValues(Block block, Object... values);

    public final void testNeverCalled() {
        SortOrder order = randomFrom(SortOrder.values());
        try (T sort = build(order, 1)) {
            assertBlock(sort, randomNonNegativeInt());
        }
    }

    public final void testSingleDoc() {
        try (T sort = build(randomFrom(SortOrder.values()), 1)) {
            collect(sort, 1, 0);

            assertBlock(sort, 0, expectedValue(1));
        }
    }

    public final void testNonCompetitive() {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 0);
            collect(sort, 1, 0);

            assertBlock(sort, 0, expectedValue(2));
        }
    }

    public final void testCompetitive() {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);

            assertBlock(sort, 0, expectedValue(2));
        }
    }

    public final void testNegativeValue() {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, -1, 0);
            assertBlock(sort, 0, expectedValue(-1));
        }
    }

    public final void testSomeBuckets() {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 0);
            collect(sort, 2, 1);
            collect(sort, 2, 2);
            collect(sort, 3, 0);

            assertBlock(sort, 0, expectedValue(3));
            assertBlock(sort, 1, expectedValue(2));
            assertBlock(sort, 2, expectedValue(2));
            assertBlock(sort, 3);
        }
    }

    public final void testBucketGaps() {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 0);
            collect(sort, 2, 2);

            assertBlock(sort, 0, expectedValue(2));
            assertBlock(sort, 1);
            assertBlock(sort, 2, expectedValue(2));
            assertBlock(sort, 3);
        }
    }

    public final void testBucketsOutOfOrder() {
        try (T sort = build(SortOrder.DESC, 1)) {
            collect(sort, 2, 1);
            collect(sort, 2, 0);

            assertBlock(sort, 0, expectedValue(2.0));
            assertBlock(sort, 1, expectedValue(2.0));
            assertBlock(sort, 2);
        }
    }

    public final void testManyBuckets() {
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
                assertBlock(sort, b, expectedValue(maxes[b]));
            }
            assertBlock(sort, buckets.length);
        }
    }

    public final void testTwoHitsDesc() {
        try (T sort = build(SortOrder.DESC, 2)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            assertBlock(sort, 0, expectedValue(3), expectedValue(2));
        }
    }

    public final void testTwoHitsAsc() {
        try (T sort = build(SortOrder.ASC, 2)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            assertBlock(sort, 0, expectedValue(1), expectedValue(2));
        }
    }

    public final void testTwoHitsTwoBucket() {
        try (T sort = build(SortOrder.DESC, 2)) {
            collect(sort, 1, 0);
            collect(sort, 1, 1);
            collect(sort, 2, 0);
            collect(sort, 2, 1);
            collect(sort, 3, 0);
            collect(sort, 3, 1);
            collect(sort, 4, 1);

            assertBlock(sort, 0, expectedValue(3), expectedValue(2));
            assertBlock(sort, 1, expectedValue(4), expectedValue(3));
        }
    }

    public final void testManyBucketsManyHits() {
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
                assertBlock(
                    sort,
                    bucket,
                    bucketValues.stream().sorted((lhs, rhs) -> rhs.compareTo(lhs)).limit(bucketSize).map(this::expectedValue).toArray()
                );
            }
            assertBlock(sort, buckets);
        }
    }

    public final void testMergeHeapToHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, 1, 0);
                collect(other, 2, 0);
                collect(other, 3, 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, expectedValue(1), expectedValue(1), expectedValue(2));
        }
    }

    public final void testMergeNoHeapToNoHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, 1, 0);
                collect(other, 2, 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, expectedValue(1), expectedValue(1), expectedValue(2));
        }
    }

    public final void testMergeHeapToNoHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, 1, 0);
                collect(other, 2, 0);
                collect(other, 3, 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, expectedValue(1), expectedValue(1), expectedValue(2));
        }
    }

    public final void testMergeNoHeapToHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(sort, 1, 0);
                collect(sort, 2, 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, expectedValue(1), expectedValue(1), expectedValue(2));
        }
    }

    public final void testMergeHeapToEmpty() {
        try (T sort = build(SortOrder.ASC, 3)) {
            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, 1, 0);
                collect(other, 2, 0);
                collect(other, 3, 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, expectedValue(1), expectedValue(2), expectedValue(3));
        }
    }

    public final void testMergeEmptyToHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            collect(sort, 1, 0);
            collect(sort, 2, 0);
            collect(sort, 3, 0);

            try (T other = build(SortOrder.ASC, 3)) {
                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, expectedValue(1), expectedValue(2), expectedValue(3));
        }
    }

    public final void testMergeEmptyToEmpty() {
        try (T sort = build(SortOrder.ASC, 3)) {
            try (T other = build(SortOrder.ASC, 3)) {
                merge(sort, 0, other, randomNonNegativeInt());
            }

            assertBlock(sort, 0);
        }
    }

    private void assertBlock(T sort, int groupId, Object... values) {
        var blockFactory = TestBlockFactory.getNonBreakingInstance();

        try (var intVector = blockFactory.newConstantIntVector(groupId, 1)) {
            var block = toBlock(sort, blockFactory, intVector);

            assertThat(block.getPositionCount(), equalTo(1));
            assertThat(block.getTotalValueCount(), equalTo(values.length));

            if (values.length == 0) {
                assertThat(block.isNull(0), equalTo(true));
            } else {
                assertBlockTypeAndValues(block, values);
            }
        }
    }

    protected final BigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }
}
