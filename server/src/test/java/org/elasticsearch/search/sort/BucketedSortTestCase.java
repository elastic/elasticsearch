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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public abstract class BucketedSortTestCase<T extends BucketedSort> extends ESTestCase {
    /**
     * Build a {@link BucketedSort} to test. Sorts built by this method shouldn't need scores.
     * @param values values to test, always sent as doubles just to have
     *        numbers to test. subclasses should cast to their favorite types
     */
    protected abstract T build(SortOrder sortOrder, DocValueFormat format, double[] values);

    /**
     * Build the expected sort value for a value.
     */
    protected abstract SortValue expectedSortValue(double v);

    private T build(SortOrder order, double[] values) {
        DocValueFormat format = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN);
        return build(order, format, values);
    }

    public final void testNeverCalled() {
        SortOrder order = randomFrom(SortOrder.values());
        DocValueFormat format = randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN);
        try (T sort = build(order, format, new double[] {})) {
            assertThat(sort.getOrder(), equalTo(order));
            assertThat(sort.getFormat(), equalTo(format));
            assertThat(sort.getValue(randomNonNegativeLong()), nullValue());
            assertFalse(sort.needsScores());
        }
    }

    public final void testEmptyLeaf() throws IOException {
        try (T sort = build(randomFrom(SortOrder.values()), new double[] {})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            assertFalse(leaf.advanceExact(0));
            assertThat(sort.getValue(randomNonNegativeLong()), nullValue());
        }
    }

    public final void testSingleDoc() throws IOException {
        try (T sort = build(randomFrom(SortOrder.values()), new double[] {1})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            assertTrue(leaf.collectIfCompetitive(0, 0));
            assertThat(sort.getValue(0), equalTo(expectedSortValue(1)));
        }
    }

    public void testNonCompetitive() throws IOException {
        try (T sort = build(SortOrder.DESC, new double[] {2, 1})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            assertTrue(leaf.collectIfCompetitive(0, 0));
            assertFalse(leaf.collectIfCompetitive(1, 0));
            assertThat(sort.getValue(0), equalTo(expectedSortValue(2)));
        }
    }

    public void testCompetitive() throws IOException {
        try (T sort = build(SortOrder.DESC, new double[] {1, 2})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            assertTrue(leaf.collectIfCompetitive(0, 0));
            assertTrue(leaf.collectIfCompetitive(1, 0));
            assertThat(sort.getValue(0), equalTo(expectedSortValue(2)));
        }
    }

    public void testNegativeValue() throws IOException {
        try (T sort = build(SortOrder.DESC, new double[] {-1})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            assertTrue(leaf.collectIfCompetitive(0, 0));
            assertThat(sort.getValue(0), equalTo(expectedSortValue(-1)));
        }
    }

    public void testSomeBuckets() throws IOException {
        try (T sort = build(SortOrder.DESC, new double[] {2, 3})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            assertTrue(leaf.collectIfCompetitive(0, 0));
            assertTrue(leaf.collectIfCompetitive(0, 1));
            assertTrue(leaf.collectIfCompetitive(0, 2));
            assertTrue(leaf.collectIfCompetitive(1, 0));
            assertThat(sort.getValue(0), equalTo(expectedSortValue(3)));
            assertThat(sort.getValue(1), equalTo(expectedSortValue(2)));
            assertThat(sort.getValue(2), equalTo(expectedSortValue(2)));
            assertThat(sort.getValue(3), nullValue());
        }
    }

    public void testBucketGaps() throws IOException {
        try (T sort = build(SortOrder.DESC, new double[] {2})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            assertTrue(leaf.collectIfCompetitive(0, 0));
            assertTrue(leaf.collectIfCompetitive(0, 2));
            assertThat(sort.getValue(0), equalTo(expectedSortValue(2)));
            assertThat(sort.getValue(1), nullValue());
            assertThat(sort.getValue(2), equalTo(expectedSortValue(2)));
            assertThat(sort.getValue(3), nullValue());
        }
    }

    public void testBucketsOutOfOrder() throws IOException {
        try (T sort = build(SortOrder.DESC, new double[] {2})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            assertTrue(leaf.collectIfCompetitive(0, 1));
            assertTrue(leaf.collectIfCompetitive(0, 0));
            assertThat(sort.getValue(0), equalTo(expectedSortValue(2.0)));
            assertThat(sort.getValue(1), equalTo(expectedSortValue(2.0)));
            assertThat(sort.getValue(2), nullValue());
        }
    }

    public void testManyBuckets() throws IOException {
        // Set the bucket values in random order
        int[] buckets = new int[10000];
        for (int b = 0; b < buckets.length; b++) {
            buckets[b] = b;
        }
        Collections.shuffle(Arrays.asList(buckets));

        double[] maxes = new double[buckets.length];

        try (T sort = build(SortOrder.DESC, new double[] {2, 3, -1})) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            for (int b : buckets) {
                maxes[b] = 2;
                assertTrue(leaf.collectIfCompetitive(0, b));
                if (randomBoolean()) {
                    maxes[b] = 3;
                    assertTrue(leaf.collectIfCompetitive(1, b));
                }
                if (randomBoolean()) {
                    assertFalse(leaf.collectIfCompetitive(2, b));
                }
            }
            for (int b = 0; b < buckets.length; b++) {
                assertThat(sort.getValue(b), equalTo(expectedSortValue(maxes[b])));
            }
            assertThat(sort.getValue(buckets.length), nullValue());
        }
    }

    protected BigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }
}
