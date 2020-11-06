/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public abstract class BucketedSortTestCase<T extends BucketedSort> extends ESTestCase {
    /**
     * Build a {@link BucketedSort} to test. Sorts built by this method shouldn't need scores.
     * @param values values to test, always sent as doubles just to have
     *        numbers to test. subclasses should cast to their favorite types
     */
    protected abstract T build(SortOrder sortOrder, DocValueFormat format, int bucketSize,
            BucketedSort.ExtraData extra, double[] values);

    /**
     * Build the expected sort value for a value.
     */
    protected abstract SortValue expectedSortValue(double v);

    /**
     * A random value for testing, with the appropriate precision for the type we're testing.
     */
    protected abstract double randomValue();

    protected final T build(SortOrder order, int bucketSize, BucketedSort.ExtraData extra, double[] values) {
        DocValueFormat format = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN);
        return build(order, format, bucketSize, extra, values);
    }

    private T build(SortOrder order, int bucketSize, double[] values) {
        DocValueFormat format = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN);
        return build(order, format, bucketSize, BucketedSort.NOOP_EXTRA_DATA, values);
    }

    public final void testNeverCalled() {
        SortOrder order = randomFrom(SortOrder.values());
        DocValueFormat format = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN);
        try (T sort = build(order, format, 1, BucketedSort.NOOP_EXTRA_DATA, new double[] {})) {
            assertThat(sort.getOrder(), equalTo(order));
            assertThat(sort.getFormat(), equalTo(format));
            assertThat(sort.getValues(randomNonNegativeLong()), empty());
            assertFalse(sort.needsScores());
        }
    }

    public final void testEmptyLeaf() throws IOException {
        try (T sort = build(randomFrom(SortOrder.values()), 1, new double[] {})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            assertThat(sort.getValues(randomNonNegativeLong()), empty());
        }
    }

    public final void testSingleDoc() throws IOException {
        try (T sort = build(randomFrom(SortOrder.values()), 1, new double[] {1})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            assertThat(sort.getValues(0), contains(expectedSortValue(1)));
        }
    }

    public void testNonCompetitive() throws IOException {
        try (T sort = build(SortOrder.DESC, 1, new double[] {2, 1})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            leaf.collect(1, 0);
            assertThat(sort.getValues(0), contains(expectedSortValue(2)));
        }
    }

    public void testCompetitive() throws IOException {
        try (T sort = build(SortOrder.DESC, 1, new double[] {1, 2})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            leaf.collect(1, 0);
            assertThat(sort.getValues(0), contains(expectedSortValue(2)));
        }
    }

    public void testNegativeValue() throws IOException {
        try (T sort = build(SortOrder.DESC, 1, new double[] {-1})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            assertThat(sort.getValues(0), contains(expectedSortValue(-1)));
        }
    }

    public void testSomeBuckets() throws IOException {
        try (Extra extra = new Extra(bigArrays(), new int[] {100, 200});
                T sort = build(SortOrder.DESC, 1, extra, new double[] {2, 3})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            leaf.collect(0, 1);
            leaf.collect(0, 2);
            leaf.collect(1, 0);

            assertThat(sort.getValues(0), contains(expectedSortValue(3)));
            assertThat(sort.getValues(1), contains(expectedSortValue(2)));
            assertThat(sort.getValues(2), contains(expectedSortValue(2)));
            assertThat(sort.getValues(3), empty());

            assertThat(sort.getValues(0, extra.valueBuilder()), contains(extraValue(200, 3)));
            assertThat(sort.getValues(1, extra.valueBuilder()), contains(extraValue(100, 2)));
            assertThat(sort.getValues(2, extra.valueBuilder()), contains(extraValue(100, 2)));
            assertThat(sort.getValues(3, extra.valueBuilder()), empty());
        }
    }

    public void testBucketGaps() throws IOException {
        try (T sort = build(SortOrder.DESC, 1, new double[] {2})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            leaf.collect(0, 2);
            assertThat(sort.getValues(0), contains(expectedSortValue(2)));
            assertThat(sort.getValues(1), empty());
            assertThat(sort.getValues(2), contains(expectedSortValue(2)));
            assertThat(sort.getValues(3), empty());
        }
    }

    public void testBucketsOutOfOrder() throws IOException {
        try (T sort = build(SortOrder.DESC, 1, new double[] {2})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 1);
            leaf.collect(0, 0);
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

        try (T sort = build(SortOrder.DESC, 1, new double[] {2, 3, -1})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            for (int b : buckets) {
                maxes[b] = 2;
                leaf.collect(0, b);
                if (randomBoolean()) {
                    maxes[b] = 3;
                    leaf.collect(1, b);
                }
                if (randomBoolean()) {
                    leaf.collect(2, b);
                }
            }
            for (int b = 0; b < buckets.length; b++) {
                assertThat(sort.getValues(b), contains(expectedSortValue(maxes[b])));
            }
            assertThat(sort.getValues(buckets.length), empty());
        }
    }

    public void testTwoHitsDesc() throws IOException {
        try (Extra extra = new Extra(bigArrays(), new int[] {100, 200, 3000});
                T sort = build(SortOrder.DESC, 2, extra, new double[] {1, 2, 3})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            leaf.collect(1, 0);
            leaf.collect(2, 0);
            assertThat(sort.getValues(0), contains(expectedSortValue(3), expectedSortValue(2)));

            assertThat(sort.getValues(0, extra.valueBuilder()), contains(extraValue(3000, 3), extraValue(200, 2)));
        }
    }
    
    public void testTwoHitsAsc() throws IOException {
        try (T sort = build(SortOrder.ASC, 2, new double[] {1, 2, 3})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            leaf.collect(1, 0);
            leaf.collect(2, 0);
            assertThat(sort.getValues(0), contains(expectedSortValue(1), expectedSortValue(2)));
        }
    }

    public void testManyHits() throws IOException {
        // Set the values in random order
        double[] values = new double[10000];
        for (int v = 0; v < values.length; v++) {
            values[v] = randomValue();
        }
        Collections.shuffle(Arrays.asList(values), random());

        int bucketSize = between(2, 1000);
        SwapCountingExtra counter = new SwapCountingExtra();
        try (T sort = build(SortOrder.DESC, bucketSize, counter, values)) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            for (int doc = 0; doc < values.length; doc++) {
                leaf.collect(doc, 0);
            }
            assertThat(sort.getValues(0), contains(Arrays.stream(values).boxed()
                    .sorted((lhs, rhs) -> rhs.compareTo(lhs))
                    .limit(bucketSize).map(s -> equalTo(expectedSortValue(s)))
                    .collect(toList())));
            assertThat(sort.getValues(1), empty());
        }
        // We almost always *way* undershoot this value.
        assertThat(counter.count, lessThan((long)(bucketSize + values.length * Math.log(bucketSize) / Math.log(2))));
    }

    public void testTwoHitsTwoBucket() throws IOException {
        try (T sort = build(SortOrder.DESC, 2, new double[] {1, 2, 3, 4})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            leaf.collect(0, 0);
            leaf.collect(0, 1);
            leaf.collect(1, 0);
            leaf.collect(1, 1);
            leaf.collect(2, 0);
            leaf.collect(2, 1);
            leaf.collect(3, 1);
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
        try (T sort = build(SortOrder.DESC, bucketSize, values)) {
            BitArray[] bucketUsed = new BitArray[buckets];
            Arrays.setAll(bucketUsed, i -> new BitArray(values.length, bigArrays()));
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            for (int doc = 0; doc < values.length; doc++) {
                for (int bucket = 0; bucket < buckets; bucket++) {
                    if (randomBoolean()) {
                        bucketUsed[bucket].set(doc);
                        leaf.collect(doc, bucket);
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
                assertThat("Bucket " + bucket, sort.getValues(bucket), contains(bucketValues.stream()
                        .sorted((lhs, rhs) -> rhs.compareTo(lhs))
                        .limit(bucketSize).map(s -> equalTo(expectedSortValue(s)))
                        .collect(toList())));
            }
            assertThat(sort.getValues(buckets), empty());
        }
    }

    protected BigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private Extra.Value extraValue(int extra, double sort) {
        return new Extra.Value(extra, expectedSortValue(sort));
    }

    private static class Extra implements BucketedSort.ExtraData, Releasable {
        private static class Value implements Comparable<Value> {
            private final int extra;
            private final SortValue sortValue;

            Value(int extra, SortValue sortValue) {
                this.extra = extra;
                this.sortValue = sortValue;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || obj.getClass() != getClass()) {
                    return false;
                }
                Value other = (Value) obj;
                return extra == other.extra && sortValue.equals(other.sortValue);
            }

            @Override
            public int hashCode() {
                return Objects.hash(extra, sortValue);
            }

            @Override
            public int compareTo(Value o) {
                return sortValue.compareTo(o.sortValue);
            }

            @Override
            public String toString() {
                return "[" + extra + "," + sortValue + "]";
            }
        }

        private final BigArrays bigArrays;
        private final int[] docValues;
        private IntArray values;

        Extra(BigArrays bigArrays, int[] docValues) {
            this.bigArrays = bigArrays;
            this.docValues = docValues;
            values = bigArrays.newIntArray(1, false);
        }

        public BucketedSort.ResultBuilder<Value> valueBuilder() {
            return (i, sv) -> new Value(values.get(i), sv);
        }

        @Override
        public void swap(long lhs, long rhs) {
            int tmp = values.get(lhs);
            values.set(lhs, values.get(rhs));
            values.set(rhs, tmp);
        }

        @Override
        public Loader loader(LeafReaderContext ctx) throws IOException {
            return (index, doc) -> {
                values = bigArrays.grow(values, index + 1);
                values.set(index, docValues[doc]);
            };
        }

        @Override
        public void close() {
            values.close();
        }
    }

    private class SwapCountingExtra implements BucketedSort.ExtraData {
        private long count = 0;

        @Override
        public void swap(long lhs, long rhs) {
            count++;
        }

        @Override
        public Loader loader(LeafReaderContext ctx) throws IOException {
            return (index, doc) -> {};
        }
    }
}
