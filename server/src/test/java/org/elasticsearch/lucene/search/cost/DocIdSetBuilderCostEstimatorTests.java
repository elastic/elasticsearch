/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class DocIdSetBuilderCostEstimatorTests extends ESTestCase {

    public void testEmptyLeafCostsNothing() {
        assertEquals(0L, new DocIdSetBuilderCostEstimator(randomLongBetween(0, Long.MAX_VALUE), 0).estimate());
    }

    public void testNothingGrownCostsNothing() {
        assertEquals(0L, new DocIdSetBuilderCostEstimator(randomLongBetween(Long.MIN_VALUE, 0), between(1, 1 << 20)).estimate());
    }

    public void testDenseOnceGrowthCanReachThreshold() {
        int maxDoc = 1 << 20;
        long threshold = maxDoc >>> 7;
        // sparse while ALLOCATION_FACTOR * grown + MIN_BUFFER_SLACK + grown stays within maxDoc >>> 7
        long lastSparse = (threshold - DocIdSetBuilderCostEstimator.MIN_BUFFER_SLACK) / (DocIdSetBuilderCostEstimator.ALLOCATION_FACTOR
            + 1);
        DocIdSetBuilderCostEstimator sparse = new DocIdSetBuilderCostEstimator(lastSparse, maxDoc);
        DocIdSetBuilderCostEstimator dense = new DocIdSetBuilderCostEstimator(lastSparse + 1, maxDoc);
        assertFalse(sparse.isDense());
        assertTrue(dense.isDense());
        long bitSetBytes = (long) FixedBitSet.bits2words(maxDoc) * Long.BYTES;
        assertThat(dense.estimate(), greaterThan(bitSetBytes));
        assertThat("the sparse buffers stay well below the bit set", sparse.estimate(), greaterThan(0L));
        assertTrue(sparse.estimate() < bitSetBytes);
    }

    public void testHugeGrowthSaturatesToDense() {
        int maxDoc = between(1, Integer.MAX_VALUE);
        DocIdSetBuilderCostEstimator estimator = new DocIdSetBuilderCostEstimator(Long.MAX_VALUE, maxDoc);
        assertTrue(estimator.isDense());
        assertThat(estimator.estimate(), greaterThanOrEqualTo((long) FixedBitSet.bits2words(maxDoc) * Long.BYTES));
    }

    /**
     * Fills a real {@link DocIdSetBuilder} the way a BKD intersection does, growing by a block of points and then adding some
     * of them, and checks that the estimate for the total grown is a ceiling on what the builder holds before {@code build()}
     * and on the set it builds, and that the builder never upgrades to a bit set when the estimate assumed it stays sparse.
     */
    public void testEstimateIsCeilingOnRealBuilder() {
        for (int iter = 0; iter < 20; iter++) {
            int maxDoc = between(1, 1 << 20);
            long threshold = maxDoc >>> 7;
            long target = randomLongBetween(1, Math.max(1, 2 * threshold));
            DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
            long grown = 0;
            int nextDoc = 0;
            while (grown < target) {
                int count = randomBoolean() ? between(1, 512) : (int) randomLongBetween(1, target);
                DocIdSetBuilder.BulkAdder adder = builder.grow(count);
                grown += count;
                int adds = Math.min(between(0, count), maxDoc - nextDoc);
                for (int i = 0; i < adds; i++) {
                    adder.add(nextDoc++);
                }
            }
            DocIdSetBuilderCostEstimator estimator = new DocIdSetBuilderCostEstimator(grown, maxDoc);
            long estimated = estimator.estimate();
            long beforeBuild = RamUsageTester.ramUsed(builder);
            DocIdSet built = builder.build();
            long afterBuild = RamUsageTester.ramUsed(built);
            String context = String.format(
                Locale.ROOT,
                "[maxDoc=%d, grown=%d, dense=%b, estimated=%d, beforeBuild=%d, afterBuild=%d]",
                maxDoc,
                grown,
                estimator.isDense(),
                estimated,
                beforeBuild,
                afterBuild
            );
            assertThat("estimate must be a ceiling on the builder " + context, estimated, greaterThanOrEqualTo(beforeBuild));
            assertThat("estimate must be a ceiling on the built set " + context, estimated, greaterThanOrEqualTo(afterBuild));
            if (estimator.isDense() == false) {
                assertThat("a sparse estimate must not see a bit set " + context, built, not(instanceOf(BitDocIdSet.class)));
            }
        }
    }
}
