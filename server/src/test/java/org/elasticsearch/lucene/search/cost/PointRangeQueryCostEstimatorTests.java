/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class PointRangeQueryCostEstimatorTests extends ESTestCase {

    public void testConstructorRejectsNegativeArguments() {
        expectThrows(IllegalArgumentException.class, () -> new PointRangeQueryCostEstimator(-1, 4, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new PointRangeQueryCostEstimator(1, -1, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> new PointRangeQueryCostEstimator(1, 4, -1, 1));
        expectThrows(IllegalArgumentException.class, () -> new PointRangeQueryCostEstimator(1, 4, 1, -1));
    }

    public void testEstimateIsAtLeastTheFloor() {
        long zeroWidth = new PointRangeQueryCostEstimator(0, 0, 0, 0).estimate();
        assertThat(zeroWidth, greaterThanOrEqualTo(PointRangeQueryCostEstimator.BASE_BYTES));
        long oneDim = new PointRangeQueryCostEstimator(1, 4, 0, 0).estimate();
        assertThat(oneDim, greaterThanOrEqualTo(PointRangeQueryCostEstimator.BASE_BYTES));
    }

    public void testEstimateIsMonotonicInBytesPerDim() {
        long narrow = new PointRangeQueryCostEstimator(1, 4, 0, 0).estimate();
        long wide = new PointRangeQueryCostEstimator(1, 16, 0, 0).estimate();
        assertTrue("estimate must grow with bytesPerDim", narrow < wide);
    }

    public void testEstimateIsMonotonicInNumDims() {
        long oneDim = new PointRangeQueryCostEstimator(1, 4, 0, 0).estimate();
        long fourDim = new PointRangeQueryCostEstimator(4, 4, 0, 0).estimate();
        long eightDim = new PointRangeQueryCostEstimator(8, 4, 0, 0).estimate();
        assertTrue(oneDim < fourDim);
        assertTrue(fourDim < eightDim);
    }

    public void testExecutionTermIsZeroWithoutSegmentInfo() {
        long structural = new PointRangeQueryCostEstimator(1, 4, 0, 0).estimate();
        assertEquals(structural, new PointRangeQueryCostEstimator(1, 4, 0, 0).estimate());
        assertEquals("maxDoc without segments adds nothing", structural, new PointRangeQueryCostEstimator(1, 4, 1000000, 0).estimate());
        assertEquals("segments without maxDoc adds nothing", structural, new PointRangeQueryCostEstimator(1, 4, 0, 5).estimate());
    }

    public void testEstimateGrowsWithMaxDoc() {
        long small = new PointRangeQueryCostEstimator(1, 4, 1000, 1).estimate();
        long large = new PointRangeQueryCostEstimator(1, 4, 1000000, 1).estimate();
        assertTrue("execution estimate must grow with maxDoc", small < large);
        assertTrue(
            "execution estimate must exceed the structural-only estimate",
            small > new PointRangeQueryCostEstimator(1, 4, 0, 0).estimate()
        );
    }

    public void testEstimateGrowsWithNumSegments() {
        long oneSegment = new PointRangeQueryCostEstimator(1, 4, 1000000, 1).estimate();
        long tenSegments = new PointRangeQueryCostEstimator(1, 4, 1000000, 10).estimate();
        assertTrue("execution estimate must grow with the number of segments", oneSegment < tenSegments);
    }

    public void testEstimateIsCeilingOnMeasuredRam() throws IOException {
        Query[] queries = {
            IntPoint.newRangeQuery("f", 1, 100),
            LongPoint.newRangeQuery("f", 1L, 100L),
            DoublePoint.newRangeQuery("f", 1.0, 100.0),
            IntPoint.newRangeQuery("f", new int[] { 0, 0 }, new int[] { 10, 10 }),
            IntPoint.newRangeQuery("f", new int[] { 0, 0, 0, 0 }, new int[] { 1, 1, 1, 1 }),
            IntPoint.newRangeQuery("f", new int[] { 0, 0, 0, 0, 0, 0, 0, 0 }, new int[] { 1, 1, 1, 1, 1, 1, 1, 1 }),
            LongPoint.newRangeQuery("f", new long[] { 0, 0, 0, 0, 0, 0, 0, 0 }, new long[] { 1, 1, 1, 1, 1, 1, 1, 1 }), };

        for (Query q : queries) {
            assertTrue("expected a PointRangeQuery, got " + q.getClass(), q instanceof PointRangeQuery);
            PointRangeQuery prq = (PointRangeQuery) q;
            long structural = RamUsageEstimator.shallowSizeOf(prq) + RamUsageEstimator.sizeOf(prq.getLowerPoint()) + RamUsageEstimator
                .sizeOf(prq.getUpperPoint());
            // maxDoc==0 exercises the structural-only estimate; the rest add the measured execution-time RAM.
            for (int maxDoc : new int[] { 0, 1, 100, 10_000, 1_000_000 }) {
                int numSegments = maxDoc == 0 ? 0 : 1;
                long estimated = new PointRangeQueryCostEstimator(prq.getNumDims(), prq.getBytesPerDim(), maxDoc, numSegments).estimate();
                long measured = maxDoc == 0 ? structural : structural + denseDocIdSetRamBytes(maxDoc);
                assertThat(
                    String.format(
                        Locale.ROOT,
                        "estimate must be a ceiling on structural+execution RAM "
                            + "[numDims=%d, bytesPerDim=%d, maxDoc=%d, estimated=%d, measured=%d]",
                        prq.getNumDims(),
                        prq.getBytesPerDim(),
                        maxDoc,
                        estimated,
                        measured
                    ),
                    estimated,
                    greaterThanOrEqualTo(measured)
                );
            }
        }
    }

    private static long denseDocIdSetRamBytes(int maxDoc) throws IOException {
        DocIdSetBuilder builder = new DocIdSetBuilder(maxDoc);
        DocIdSetBuilder.BulkAdder adder = builder.grow(maxDoc);
        for (int doc = 0; doc < maxDoc; doc++) {
            adder.add(doc);
        }
        DocIdSet docIdSet = builder.build();
        return docIdSet.ramBytesUsed();
    }
}
