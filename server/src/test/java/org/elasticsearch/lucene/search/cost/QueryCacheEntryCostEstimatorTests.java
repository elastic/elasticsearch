/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;
import java.util.function.IntPredicate;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class QueryCacheEntryCostEstimatorTests extends ESTestCase {

    private static final int BLOCK = 1 << 16;

    public void testEmptyLeafCostsNothing() {
        assertEquals(0L, new QueryCacheEntryCostEstimator(randomLong(), 0).estimate());
    }

    /**
     * Builds what {@code LRUQueryCache.cacheImpl} builds for a clause whose cost is its exact match count, a dense
     * {@link BitDocIdSet} when {@code cost * 100 >= maxDoc} and otherwise a {@link RoaringDocIdSet} in every block layout
     * Lucene can choose, and checks the estimate is a ceiling on its {@code ramBytesUsed()}. For roaring sets the builder
     * scratch, which the finished set no longer holds, is left out so the structural part of the estimate is held to the
     * measurement.
     */
    public void testEstimateIsCeilingOnMeasuredRam() {
        int maxDoc = 3 * BLOCK + between(1, BLOCK - 1);
        // doc selection patterns: a sparse stride, a dense stride, a single block dense enough for a bitset, a single block
        // almost full (NotDocIdSet), a single block completely full, everything, and a random stride
        int stride = between(1, 20_000);
        IntPredicate[] patterns = new IntPredicate[] {
            doc -> doc % 1000 == 0,
            doc -> doc % 17 == 0,
            doc -> doc / BLOCK == 1 && doc % 3 == 0,
            doc -> doc / BLOCK == 2 && doc % 1000 != 0,
            doc -> doc / BLOCK == 0,
            doc -> true,
            doc -> doc % stride == 0 };
        for (IntPredicate pattern : patterns) {
            RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
            int matches = 0;
            for (int doc = 0; doc < maxDoc; doc++) {
                if (pattern.test(doc)) {
                    builder.add(doc);
                    matches++;
                }
            }
            long estimated = new QueryCacheEntryCostEstimator(matches, maxDoc).estimate();
            final boolean dense = (long) matches * 100 >= maxDoc;
            final long measured;
            if (dense) {
                measured = new BitDocIdSet(new FixedBitSet(maxDoc)).ramBytesUsed();
            } else {
                measured = builder.build().ramBytesUsed();
                estimated -= QueryCacheEntryCostEstimator.ROARING_BUILDER_SCRATCH_BYTES;
            }
            assertThat(
                String.format(
                    Locale.ROOT,
                    "estimate must be a ceiling on the built set [maxDoc=%d, matches=%d, dense=%b, estimated=%d, measured=%d]",
                    maxDoc,
                    matches,
                    dense,
                    estimated,
                    measured
                ),
                estimated,
                greaterThanOrEqualTo(measured)
            );
        }
    }
}
