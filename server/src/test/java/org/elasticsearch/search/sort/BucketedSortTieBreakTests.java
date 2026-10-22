/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the optional tie-breaking of {@link BucketedSort}. A Lucene {@code TopFieldCollector} keeps the lower doc id among
 * entries with equal sort values, so a caller that has to match it needs the tie-break inside the heap: once a tied entry has
 * been evicted, no amount of sorting the results can bring it back.
 */
public class BucketedSortTieBreakTests extends ESTestCase {
    private final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    /**
     * Stores the doc id next to each sort value, and breaks ties on it only when asked to.
     */
    private static class DocIds implements BucketedSort.ExtraData, Releasable {
        private final BigArrays bigArrays;
        IntArray docIds;

        DocIds(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.docIds = bigArrays.newIntArray(1, false);
        }

        @Override
        public void swap(long lhs, long rhs) {
            int tmp = docIds.get(lhs);
            docIds.set(lhs, docIds.get(rhs));
            docIds.set(rhs, tmp);
        }

        @Override
        public Loader loader(LeafReaderContext ctx) {
            return (index, doc) -> {
                docIds = bigArrays.grow(docIds, index + 1);
                docIds.set(index, doc);
            };
        }

        @Override
        public void close() {
            docIds.close();
        }
    }

    private static class TieBreakingDocIds extends DocIds implements BucketedSort.TieBreakingExtraData {
        TieBreakingDocIds(BigArrays bigArrays) {
            super(bigArrays);
        }

        @Override
        public boolean betterOnTie(long lhs, long rhs) {
            return docIds.get(lhs) < docIds.get(rhs);
        }
    }

    /**
     * A result that orders by sort value and then by doc id, lowest first whatever the sort order. {@link BucketedSort#getValues}
     * reverses the whole comparison for a descending sort, so the doc id comparison is reversed in that case.
     */
    private record Hit(SortValue value, int doc, SortOrder order) implements Comparable<Hit> {
        @Override
        public int compareTo(Hit other) {
            int cmp = value.compareTo(other.value);
            if (cmp != 0) {
                return cmp;
            }
            return order == SortOrder.DESC ? Integer.compare(other.doc, doc) : Integer.compare(doc, other.doc);
        }
    }

    private BucketedSort.ForLongs sort(SortOrder order, int bucketSize, DocIds extra, long[] values) {
        return new BucketedSort.ForLongs(bigArrays, order, DocValueFormat.RAW, bucketSize, extra) {
            @Override
            public Leaf forLeaf(LeafReaderContext ctx) {
                return new Leaf(ctx) {
                    private int index = -1;

                    @Override
                    protected boolean advanceExact(int doc) {
                        index = doc;
                        return doc < values.length;
                    }

                    @Override
                    protected long docValue() {
                        return values[index];
                    }
                };
            }
        };
    }

    private List<Integer> collect(BucketedSort sort, DocIds extra, long[] values, long bucket) throws IOException {
        BucketedSort.Leaf leaf = sort.forLeaf(null);
        for (int doc = 0; doc < values.length; doc++) {
            leaf.collect(doc, bucket);
        }
        return docs(sort, extra, bucket);
    }

    private List<Integer> docs(BucketedSort sort, DocIds extra, long bucket) throws IOException {
        List<Integer> docs = new ArrayList<>();
        for (Hit hit : sort.getValues(bucket, (index, value) -> new Hit(value, extra.docIds.get(index), sort.getOrder()))) {
            docs.add(hit.doc);
        }
        return docs;
    }

    /**
     * Values 6, 5, 5, 4, 3 in doc id order, keeping the best three ascending. The two 5s are docs 1 and 2 and only one fits, and
     * the one to keep is doc 1.
     */
    public void testAscendingKeepsLowerDocIdOnTie() throws IOException {
        long[] values = new long[] { 6, 5, 5, 4, 3 };
        try (TieBreakingDocIds extra = new TieBreakingDocIds(bigArrays); BucketedSort sort = sort(SortOrder.ASC, 3, extra, values)) {
            assertThat(collect(sort, extra, values, 0), contains(4, 3, 1));
        }
    }

    /**
     * The mirror image: the best three descending of 3, 4, 4, 5, 6 are 6, 5 and one of the 4s, which are docs 1 and 2.
     */
    public void testDescendingKeepsLowerDocIdOnTie() throws IOException {
        long[] values = new long[] { 3, 4, 4, 5, 6 };
        try (TieBreakingDocIds extra = new TieBreakingDocIds(bigArrays); BucketedSort sort = sort(SortOrder.DESC, 3, extra, values)) {
            assertThat(collect(sort, extra, values, 0), contains(4, 3, 1));
        }
    }

    /**
     * Without a tie-breaking {@code ExtraData} the sort behaves as it always has, and for the case above it evicts doc 1. This is
     * what {@code top_metrics} gets, and it shows the tests above would fail without the tie-break.
     */
    public void testWithoutTieBreakerBehaviourIsUnchanged() throws IOException {
        long[] values = new long[] { 6, 5, 5, 4, 3 };
        try (DocIds extra = new DocIds(bigArrays); BucketedSort sort = sort(SortOrder.ASC, 3, extra, values)) {
            assertThat(collect(sort, extra, values, 0), contains(4, 3, 2));
        }
    }

    /**
     * Random values from a small range, so there are many ties, spread over a few buckets, must match a stable sort by value
     * and then by doc id.
     */
    public void testMatchesStableSortWithManyTies() throws IOException {
        SortOrder order = randomFrom(SortOrder.values());
        int bucketSize = between(1, 8);
        int bucketCount = between(1, 4);
        long[] values = new long[between(0, 80)];
        long[] buckets = new long[values.length];
        for (int doc = 0; doc < values.length; doc++) {
            values[doc] = between(0, 5);
            buckets[doc] = between(0, bucketCount - 1);
        }
        try (TieBreakingDocIds extra = new TieBreakingDocIds(bigArrays); BucketedSort sort = sort(order, bucketSize, extra, values)) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            for (int doc = 0; doc < values.length; doc++) {
                leaf.collect(doc, buckets[doc]);
            }
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                List<Integer> expected = new ArrayList<>();
                for (int doc = 0; doc < values.length; doc++) {
                    if (buckets[doc] == bucket) {
                        expected.add(doc);
                    }
                }
                Comparator<Integer> byValue = Comparator.comparingLong(doc -> values[doc]);
                expected.sort((order == SortOrder.DESC ? byValue.reversed() : byValue).thenComparingInt(doc -> doc));
                assertThat(docs(sort, extra, bucket), equalTo(expected.subList(0, Math.min(bucketSize, expected.size()))));
            }
        }
    }
}
