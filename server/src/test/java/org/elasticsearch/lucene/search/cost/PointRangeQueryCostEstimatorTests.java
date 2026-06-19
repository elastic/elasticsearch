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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class PointRangeQueryCostEstimatorTests extends ESTestCase {

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
        assertEquals(0L, PointRangeQueryCostEstimator.executionBytesForLeaf(1000000, 1000000, true, true, 1, 4));
    }

    public void testExecutionBytesForEmptyLeafIsZero() {
        assertEquals(0L, PointRangeQueryCostEstimator.executionBytesForLeaf(0, 0, true, false, 1, 4));
    }

    public void testExecutionBytesDenseSingleValuedAllocatesFullBitset() {
        int maxDoc = 1000000;
        long dense = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc, true, false, 1, 4);
        long expectedBitset = Math.ceilDiv(maxDoc, 64L) * 8L + PointRangeQueryCostEstimator.FIXED_BITSET_BASE_BYTES;
        long bkdScratch = PointRangeQueryCostEstimator.MAX_POINTS_IN_LEAF_NODE * 4L;
        assertEquals(expectedBitset + bkdScratch, dense);
    }

    public void testExecutionBytesSelectiveIsCheaperThanDense() {
        int maxDoc = 1000000;
        long dense = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc, true, false, 1, 4);
        long selective = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc / 1000, maxDoc, true, false, 1, 4);
        assertTrue("a selective range must be charged less than the dense worst case", selective < dense);
    }

    public void testExecutionBytesIsCappedByDenseBitset() {
        int maxDoc = 1000000;
        long dense = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc, false, false, 1, 4);
        long hugeCost = PointRangeQueryCostEstimator.executionBytesForLeaf(Long.MAX_VALUE / 8, maxDoc, false, false, 1, 4);
        assertEquals(dense, hugeCost);
    }

    public void testExecutionBytesMultiValuedNeverUsesDensePathDirectly() {
        int maxDoc = 1000000;
        long multiValued = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc, false, false, 1, 4);
        long singleValuedDense = PointRangeQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc, true, false, 1, 4);
        assertEquals(singleValuedDense, multiValued);
    }

    public void testExecutionBytesSaturatesOnOverflow() {
        assertEquals(
            Long.MAX_VALUE,
            PointRangeQueryCostEstimator.executionBytesForLeaf(1, Integer.MAX_VALUE, true, false, Integer.MAX_VALUE, Integer.MAX_VALUE)
        );
    }

    public void testExecutionBytesIsCeilingOnMeasuredRam() throws IOException {
        for (int maxDoc : new int[] { 1, 100, 10000, 1000000 }) {
            for (int matches : new int[] { 1, Math.max(1, maxDoc / 1000), Math.max(1, maxDoc / 2), maxDoc }) {
                long measured = docIdSetRamBytes(maxDoc, matches);
                boolean singleValued = true;
                long estimated = PointRangeQueryCostEstimator.executionBytesForLeaf(matches, maxDoc, singleValued, false, 1, 4);
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
