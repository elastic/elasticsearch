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
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the optional tie-breaking of {@link BucketedSort}. A Lucene {@code TopFieldCollector} keeps the lower doc id among
 * entries with equal sort values, so a caller that has to match it needs the tie-break inside the heap: once a tied entry has
 * been evicted, no amount of sorting the results can bring it back. What is checked is which documents a bucket kept.
 */
public class BucketedSortTieBreakTests extends ESTestCase {
    private final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    /**
     * Stores the doc id next to each sort value, and breaks ties on it: the lower doc id wins. A test knows how many slots there
     * are, so plain arrays will do.
     */
    private static class DocIds implements BucketedSort.TieBreakingExtraData {
        private final int[] docIds;

        DocIds(int slots) {
            this.docIds = new int[slots];
        }

        @Override
        public void swap(long lhs, long rhs) {
            int tmp = docIds[(int) lhs];
            docIds[(int) lhs] = docIds[(int) rhs];
            docIds[(int) rhs] = tmp;
        }

        @Override
        public Loader loader(LeafReaderContext ctx) {
            return (index, doc) -> docIds[(int) index] = doc;
        }

        @Override
        public boolean betterOnTie(long lhs, long rhs) {
            return docIds[(int) lhs] < docIds[(int) rhs];
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

    /**
     * The doc ids a bucket kept, lowest first, so that what is compared is which documents were kept and not the order of the ones
     * with equal values among them.
     */
    private static List<Integer> kept(BucketedSort sort, DocIds extra, long bucket) throws IOException {
        List<Integer> kept = new ArrayList<>(sort.getValues(bucket, (index, value) -> extra.docIds[(int) index]));
        Collections.sort(kept);
        return kept;
    }

    private List<Integer> collectIntoOneBucket(SortOrder order, int bucketSize, long[] values) throws IOException {
        DocIds extra = new DocIds(bucketSize);
        try (BucketedSort sort = sort(order, bucketSize, extra, values)) {
            BucketedSort.Leaf leaf = sort.forLeaf(null);
            for (int doc = 0; doc < values.length; doc++) {
                leaf.collect(doc, 0);
            }
            return kept(sort, extra, 0);
        }
    }

    /**
     * Values 6, 5, 5, 4, 3 in doc id order, keeping the best three ascending. The two 5s are docs 1 and 2 and only one fits, and
     * the one to keep is doc 1.
     */
    public void testAscendingKeepsLowerDocIdOnTie() throws IOException {
        assertThat(collectIntoOneBucket(SortOrder.ASC, 3, new long[] { 6, 5, 5, 4, 3 }), contains(1, 3, 4));
    }

    /**
     * The mirror image: the best three descending of 3, 4, 4, 5, 6 are 6, 5 and one of the 4s, which are docs 1 and 2.
     */
    public void testDescendingKeepsLowerDocIdOnTie() throws IOException {
        assertThat(collectIntoOneBucket(SortOrder.DESC, 3, new long[] { 3, 4, 4, 5, 6 }), contains(1, 3, 4));
    }

    /**
     * Random values from a small range, so there are many ties, spread over a few buckets, must keep what a stable sort by value
     * and then by doc id would.
     */
    public void testKeepsWhatAStableSortWouldWithManyTies() throws IOException {
        SortOrder order = randomFrom(SortOrder.values());
        int bucketSize = between(1, 8);
        int bucketCount = between(1, 4);
        long[] values = new long[between(0, 80)];
        long[] buckets = new long[values.length];
        for (int doc = 0; doc < values.length; doc++) {
            values[doc] = between(0, 5);
            buckets[doc] = between(0, bucketCount - 1);
        }
        DocIds extra = new DocIds(bucketCount * bucketSize);
        try (BucketedSort sort = sort(order, bucketSize, extra, values)) {
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
                List<Integer> expectedKept = new ArrayList<>(expected.subList(0, Math.min(bucketSize, expected.size())));
                Collections.sort(expectedKept);
                assertThat(kept(sort, extra, bucket), equalTo(expectedKept));
            }
        }
    }
}
