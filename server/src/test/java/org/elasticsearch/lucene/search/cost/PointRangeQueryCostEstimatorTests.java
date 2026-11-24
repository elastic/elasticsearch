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
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class PointRangeQueryCostEstimatorTests extends ESTestCase {

    /** Traversal scratch of a one-dimension, four-byte point range. */
    private static final long BKD_SCRATCH_BYTES = PointRangeQueryCostEstimator.MAX_POINTS_IN_LEAF_NODE * 4L;

    public void testConstructorRejectsNegativeArguments() {
        expectThrows(IllegalArgumentException.class, () -> new PointRangeQueryCostEstimator(-1, 4));
        expectThrows(IllegalArgumentException.class, () -> new PointRangeQueryCostEstimator(1, -1));
    }

    public void testEstimateIsAtLeastTheFloor() {
        long zeroWidth = new PointRangeQueryCostEstimator(0, 0).estimate();
        assertThat(zeroWidth, greaterThanOrEqualTo(PointRangeQueryCostEstimator.BASE_BYTES));
        long oneDim = new PointRangeQueryCostEstimator(1, 4).estimate();
        assertThat(oneDim, greaterThanOrEqualTo(PointRangeQueryCostEstimator.BASE_BYTES));
    }

    public void testEstimateIsStructuralOnly() {
        long structural = new PointRangeQueryCostEstimator(1, 4).estimate();
        assertEquals(structural, new PointRangeQueryCostEstimator(1, 4).estimate());
    }

    public void testEstimateIsMonotonicInBytesPerDim() {
        long narrow = new PointRangeQueryCostEstimator(1, 4).estimate();
        long wide = new PointRangeQueryCostEstimator(1, 16).estimate();
        assertTrue("estimate must grow with bytesPerDim", narrow < wide);
    }

    public void testEstimateIsMonotonicInNumDims() {
        long oneDim = new PointRangeQueryCostEstimator(1, 4).estimate();
        long fourDim = new PointRangeQueryCostEstimator(4, 4).estimate();
        long eightDim = new PointRangeQueryCostEstimator(8, 4).estimate();
        assertTrue(oneDim < fourDim);
        assertTrue(fourDim < eightDim);
    }

    public void testExecutionBytesForMatchAllIsZero() {
        int maxDoc = 1000000;
        assertEquals(0L, PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc, maxDoc, maxDoc, true, 1, 4));
    }

    /** Lucene only skips the intersection when every doc has a value; otherwise even a range covering all values fills a builder. */
    public void testExecutionBytesForMatchAllOnSparseFieldIsCharged() {
        int maxDoc = 1000000;
        int docCount = maxDoc - 1;
        long expected = DocIdSetBuilderCostEstimator.forPoints(docCount, docCount, docCount, maxDoc).estimate() + BKD_SCRATCH_BYTES;
        assertEquals(expected, PointRangeQueryCostEstimator.executionBytesForLeaf(docCount, maxDoc, docCount, docCount, true, 1, 4));
    }

    public void testExecutionBytesForEmptyLeafIsZero() {
        assertEquals(0L, PointRangeQueryCostEstimator.executionBytesForLeaf(0, 0, 0, 0, false, 1, 4));
    }

    public void testExecutionBytesDenseSingleValuedAllocatesFullBitset() {
        int maxDoc = 1000000;
        long dense = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc, maxDoc, maxDoc, false, 1, 4);
        assertEquals(FixedBitSetCost.scorerBytes(maxDoc) + BKD_SCRATCH_BYTES, dense);
    }

    /** Lucene only takes the inverse path when every doc has exactly one value; otherwise a dense range upgrades the builder. */
    public void testExecutionBytesDenseInverseNeedsEveryDocToHaveAValue() {
        int maxDoc = 1000000;
        int docCount = maxDoc - 1;
        long expected = DocIdSetBuilderCostEstimator.forPoints(docCount, docCount, docCount, maxDoc).estimate() + BKD_SCRATCH_BYTES;
        assertEquals(expected, PointRangeQueryCostEstimator.executionBytesForLeaf(docCount, maxDoc, docCount, docCount, false, 1, 4));
        assertThat(expected, greaterThan(FixedBitSetCost.bytes(maxDoc)));
    }

    public void testExecutionBytesSelectiveIsCheaperThanDense() {
        int maxDoc = 1000000;
        long dense = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc, maxDoc, maxDoc, false, 1, 4);
        long selective = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc / 10000, maxDoc, maxDoc, maxDoc, false, 1, 4);
        assertThat("a selective range must be charged less than the dense worst case", selective, lessThan(dense));
    }

    /**
     * {@code DocIdSetBuilder} upgrades to a {@code FixedBitSet} once its buffers would pass {@code maxDoc >>> 7} entries, long
     * before {@code 4 * cost} buffer bytes reach the size of that bit set at {@code cost = maxDoc / 32}.
     */
    public void testExecutionBytesChargesBitSetPastBuilderThreshold() {
        int maxDoc = 1000000;
        long cost = randomLongBetween((maxDoc >>> 7) + 1, maxDoc / 32);
        long charged = PointRangeQueryCostEstimator.executionBytesForLeaf(cost, maxDoc, maxDoc, maxDoc, false, 1, 4);
        assertThat(charged, greaterThanOrEqualTo(FixedBitSetCost.bytes(maxDoc) + BKD_SCRATCH_BYTES));
    }

    /** The builder buffers one entry per point, so on a multivalued field it reaches its threshold at fewer matching docs. */
    public void testExecutionBytesScalesWithValuesPerDoc() {
        int maxDoc = 1000000;
        int valuesPerDoc = 4;
        long multiValuedSize = (long) valuesPerDoc * maxDoc;
        long selective = maxDoc / 10000;
        long singleValued = PointRangeQueryCostEstimator.executionBytesForLeaf(selective, maxDoc, maxDoc, maxDoc, false, 1, 4);
        long multiValued = PointRangeQueryCostEstimator.executionBytesForLeaf(selective, maxDoc, multiValuedSize, maxDoc, false, 1, 4);
        assertThat(multiValued, greaterThan(singleValued));

        long pastThreshold = (maxDoc >>> 7) / valuesPerDoc + 1;
        assertThat(
            PointRangeQueryCostEstimator.executionBytesForLeaf(pastThreshold, maxDoc, multiValuedSize, maxDoc, false, 1, 4),
            greaterThanOrEqualTo(FixedBitSetCost.bytes(maxDoc) + BKD_SCRATCH_BYTES)
        );
    }

    public void testExecutionBytesIsCappedByDenseBuilder() {
        int maxDoc = 1000000;
        long size = 2L * maxDoc;
        long dense = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc, size, maxDoc, false, 1, 4);
        long hugeCost = PointRangeQueryCostEstimator.executionBytesForLeaf(Long.MAX_VALUE / 8, maxDoc, size, maxDoc, false, 1, 4);
        assertEquals(dense, hugeCost);
    }

    public void testExecutionBytesSaturatesOnOverflow() {
        assertEquals(
            Long.MAX_VALUE,
            PointRangeQueryCostEstimator.executionBytesForLeaf(
                1,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                false,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE
            )
        );
    }

    public void testExecutionBytesIsCeilingOnMeasuredRam() throws IOException {
        for (int maxDoc : new int[] { 1, 100, 10000, 1000000 }) {
            for (int matches : new int[] { 1, Math.max(1, maxDoc / 1000), Math.max(1, maxDoc / 2), maxDoc }) {
                long measured = docIdSetRamBytes(maxDoc, matches);
                long estimated = PointRangeQueryCostEstimator.executionBytesForLeaf(matches, maxDoc, maxDoc, maxDoc, false, 1, 4);
                assertThat(
                    String.format(
                        Locale.ROOT,
                        "per-leaf estimate must be a ceiling on the materialised DocIdSet RAM "
                            + "[maxDoc=%d, matches=%d, estimated=%d, measured=%d]",
                        maxDoc,
                        matches,
                        estimated,
                        measured
                    ),
                    estimated,
                    greaterThanOrEqualTo(measured)
                );
            }
        }
    }

    private static long docIdSetRamBytes(int maxDoc, int matches) throws IOException {
        DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
        DocIdSetBuilder.BulkAdder adder = builder.grow(matches);
        int step = Math.max(1, maxDoc / matches);
        int added = 0;
        for (int doc = 0; doc < maxDoc && added < matches; doc += step) {
            adder.add(doc);
            added++;
        }
        DocIdSet docIdSet = builder.build();
        return docIdSet.ramBytesUsed();
    }
}
