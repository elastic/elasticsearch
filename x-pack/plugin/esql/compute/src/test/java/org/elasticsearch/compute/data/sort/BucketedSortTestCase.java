/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public abstract class BucketedSortTestCase<T extends Releasable, V extends Comparable<V>> extends ESTestCase {
    /**
     * Build a {@link T} to test. Sorts built by this method shouldn't need scores.
     */
    protected abstract T build(SortOrder sortOrder, int bucketSize);

    /**
     * A random value for testing, with the appropriate precision for the type we're testing.
     */
    protected abstract V randomValue();

    /**
     * Returns a list of 3 values, in ascending order.
     */
    protected abstract List<V> threeSortedValues();

    /**
     * Collect a value into the sort.
     * @param value value to collect, always sent as double just to have
     *        a number to test. Subclasses should cast to their favorite types
     */
    protected abstract void collect(T sort, V value, int bucket);

    protected abstract void merge(T sort, int groupId, T other, int otherGroupId);

    protected abstract Block toBlock(T sort, BlockFactory blockFactory, IntVector selected);

    protected abstract void assertBlockTypeAndValues(Block block, List<V> values);

    public final void testNeverCalled() {
        SortOrder order = randomFrom(SortOrder.values());
        try (T sort = build(order, 1)) {
            assertBlock(sort, randomNonNegativeInt(), List.of());
        }
    }

    public final void testSingleDoc() {
        try (T sort = build(randomFrom(SortOrder.values()), 1)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);

            assertBlock(sort, 0, List.of(values.get(0)));
        }
    }

    public final void testNonCompetitive() {
        try (T sort = build(SortOrder.DESC, 1)) {
            var values = threeSortedValues();

            collect(sort, values.get(1), 0);
            collect(sort, values.get(0), 0);

            assertBlock(sort, 0, List.of(values.get(1)));
        }
    }

    public final void testCompetitive() {
        try (T sort = build(SortOrder.DESC, 1)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 0);

            assertBlock(sort, 0, List.of(values.get(1)));
        }
    }

    public final void testSomeBuckets() {
        try (T sort = build(SortOrder.DESC, 1)) {
            var values = threeSortedValues();

            collect(sort, values.get(1), 0);
            collect(sort, values.get(1), 1);
            collect(sort, values.get(1), 2);
            collect(sort, values.get(2), 0);

            assertBlock(sort, 0, List.of(values.get(2)));
            assertBlock(sort, 1, List.of(values.get(1)));
            assertBlock(sort, 2, List.of(values.get(1)));
            assertBlock(sort, 3, List.of());
        }
    }

    public final void testBucketGaps() {
        try (T sort = build(SortOrder.DESC, 1)) {
            var values = threeSortedValues();

            collect(sort, values.get(1), 0);
            collect(sort, values.get(1), 2);

            assertBlock(sort, 0, List.of(values.get(1)));
            assertBlock(sort, 1, List.of());
            assertBlock(sort, 2, List.of(values.get(1)));
            assertBlock(sort, 3, List.of());
        }
    }

    public final void testBucketsOutOfOrder() {
        try (T sort = build(SortOrder.DESC, 1)) {
            var values = threeSortedValues();

            collect(sort, values.get(1), 1);
            collect(sort, values.get(1), 0);

            assertBlock(sort, 0, List.of(values.get(1)));
            assertBlock(sort, 1, List.of(values.get(1)));
            assertBlock(sort, 2, List.of());
        }
    }

    public final void testManyBuckets() {
        // Collect the buckets in random order
        int[] buckets = new int[10000];
        for (int b = 0; b < buckets.length; b++) {
            buckets[b] = b;
        }
        Collections.shuffle(Arrays.asList(buckets), random());

        var values = threeSortedValues();
        List<V> maxes = new ArrayList<V>(Collections.nCopies(buckets.length, null));

        try (T sort = build(SortOrder.DESC, 1)) {
            for (int b : buckets) {
                maxes.set(b, values.get(1));
                collect(sort, values.get(1), b);
                if (randomBoolean()) {
                    maxes.set(b, values.get(2));
                    collect(sort, values.get(2), b);
                }
                if (randomBoolean()) {
                    collect(sort, values.get(0), b);
                }
            }
            for (int b = 0; b < buckets.length; b++) {
                assertBlock(sort, b, List.of(maxes.get(b)));
            }
            assertBlock(sort, buckets.length, List.of());
        }
    }

    public final void testTwoHitsDesc() {
        try (T sort = build(SortOrder.DESC, 2)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 0);
            collect(sort, values.get(2), 0);

            assertBlock(sort, 0, List.of(values.get(2), values.get(1)));
        }
    }

    public final void testTwoHitsAsc() {
        try (T sort = build(SortOrder.ASC, 2)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 0);
            collect(sort, values.get(2), 0);

            assertBlock(sort, 0, List.of(values.get(0), values.get(1)));
        }
    }

    public final void testTwoHitsTwoBucket() {
        try (T sort = build(SortOrder.DESC, 2)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(0), 1);
            collect(sort, values.get(1), 0);
            collect(sort, values.get(1), 1);
            collect(sort, values.get(2), 0);

            assertBlock(sort, 0, List.of(values.get(2), values.get(1)));
            assertBlock(sort, 1, List.of(values.get(1), values.get(0)));
        }
    }

    public final void testManyBucketsManyHits() {
        // Set the values in random order
        List<V> values = new ArrayList<V>();
        for (int v = 0; v < 10000; v++) {
            values.add(randomValue());
        }
        Collections.shuffle(values, random());

        int buckets = between(2, 100);
        int bucketSize = between(2, 100);
        try (T sort = build(SortOrder.DESC, bucketSize)) {
            BitArray[] bucketUsed = new BitArray[buckets];
            Arrays.setAll(bucketUsed, i -> new BitArray(values.size(), bigArrays()));
            for (int doc = 0; doc < values.size(); doc++) {
                for (int bucket = 0; bucket < buckets; bucket++) {
                    if (randomBoolean()) {
                        bucketUsed[bucket].set(doc);
                        collect(sort, values.get(doc), bucket);
                    }
                }
            }
            for (int bucket = 0; bucket < buckets; bucket++) {
                List<V> bucketValues = new ArrayList<>(values.size());
                for (int doc = 0; doc < values.size(); doc++) {
                    if (bucketUsed[bucket].get(doc)) {
                        bucketValues.add(values.get(doc));
                    }
                }
                bucketUsed[bucket].close();
                assertBlock(sort, bucket, bucketValues.stream().sorted(Comparator.reverseOrder()).limit(bucketSize).toList());
            }
            assertBlock(sort, buckets, List.of());
        }
    }

    public final void testMergeHeapToHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 0);
            collect(sort, values.get(2), 0);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, values.get(0), 0);
                collect(other, values.get(1), 0);
                collect(other, values.get(2), 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, List.of(values.get(0), values.get(0), values.get(1)));
        }
    }

    public final void testMergeNoHeapToNoHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 0);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, values.get(0), 0);
                collect(other, values.get(1), 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, List.of(values.get(0), values.get(0), values.get(1)));
        }
    }

    public final void testMergeHeapToNoHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 0);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, values.get(0), 0);
                collect(other, values.get(1), 0);
                collect(other, values.get(2), 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, List.of(values.get(0), values.get(0), values.get(1)));
        }
    }

    public final void testMergeNoHeapToHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 0);
            collect(sort, values.get(2), 0);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(sort, values.get(0), 0);
                collect(sort, values.get(1), 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, List.of(values.get(0), values.get(0), values.get(1)));
        }
    }

    public final void testMergeHeapToEmpty() {
        try (T sort = build(SortOrder.ASC, 3)) {
            var values = threeSortedValues();

            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, values.get(0), 0);
                collect(other, values.get(1), 0);
                collect(other, values.get(2), 0);

                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, List.of(values.get(0), values.get(1), values.get(2)));
        }
    }

    public final void testMergeEmptyToHeap() {
        try (T sort = build(SortOrder.ASC, 3)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 0);
            collect(sort, values.get(2), 0);

            try (T other = build(SortOrder.ASC, 3)) {
                merge(sort, 0, other, 0);
            }

            assertBlock(sort, 0, List.of(values.get(0), values.get(1), values.get(2)));
        }
    }

    public final void testMergeEmptyToEmpty() {
        try (T sort = build(SortOrder.ASC, 3)) {
            try (T other = build(SortOrder.ASC, 3)) {
                merge(sort, 0, other, randomNonNegativeInt());
            }

            assertBlock(sort, 0, List.of());
        }
    }

    public final void testMergeOtherBigger() {
        try (T sort = build(SortOrder.ASC, 3)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 0);
            collect(sort, values.get(2), 0);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, values.get(0), 0);
                collect(other, values.get(1), 1);
                collect(other, values.get(2), 2);

                merge(sort, 0, other, 0);
                merge(sort, 0, other, 1);
                merge(sort, 0, other, 2);
            }

            assertBlock(sort, 0, List.of(values.get(0), values.get(0), values.get(1)));
        }
    }

    public final void testMergeThisBigger() {
        try (T sort = build(SortOrder.ASC, 3)) {
            var values = threeSortedValues();

            collect(sort, values.get(0), 0);
            collect(sort, values.get(1), 1);
            collect(sort, values.get(2), 2);

            try (T other = build(SortOrder.ASC, 3)) {
                collect(other, values.get(0), 0);
                collect(other, values.get(1), 0);
                collect(other, values.get(2), 0);

                merge(sort, 0, other, 0);
                merge(sort, 1, other, 0);
                merge(sort, 2, other, 0);
            }

            assertBlock(sort, 0, List.of(values.get(0), values.get(0), values.get(1)));
            assertBlock(sort, 1, List.of(values.get(0), values.get(1), values.get(1)));
            assertBlock(sort, 2, values);
        }
    }

    public final void testMergePastEnd() {
        int buckets = 10000;
        int bucketSize = between(1, 1000);
        int target = between(0, buckets);
        List<V> values = randomList(buckets, buckets, this::randomValue);
        Collections.sort(values);
        try (T sort = build(SortOrder.ASC, bucketSize)) {
            // Add a single value to the main sort.
            for (int b = 0; b < buckets; b++) {
                collect(sort, values.get(b), b);
            }

            try (T other = build(SortOrder.ASC, bucketSize)) {
                // Add *all* values to the target bucket of the secondary sort.
                for (int i = 0; i < values.size(); i++) {
                    if (i != target) {
                        collect(other, values.get(i), target);
                    }
                }

                // Merge all buckets pairwise. Most of the secondary ones are empty.
                for (int b = 0; b < buckets; b++) {
                    merge(sort, b, other, b);
                }
            }

            for (int b = 0; b < buckets; b++) {
                if (b == target) {
                    assertBlock(sort, b, values.subList(0, bucketSize));
                } else {
                    assertBlock(sort, b, List.of(values.get(b)));
                }
            }
        }
    }

    protected void assertBlock(T sort, int groupId, List<V> values) {
        var blockFactory = TestBlockFactory.getNonBreakingInstance();

        try (var intVector = blockFactory.newConstantIntVector(groupId, 1)) {
            var block = toBlock(sort, blockFactory, intVector);

            assertThat(block.getPositionCount(), equalTo(1));
            assertThat(block.getTotalValueCount(), equalTo(values.size()));

            if (values.isEmpty()) {
                assertThat(block.elementType(), equalTo(ElementType.NULL));
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
