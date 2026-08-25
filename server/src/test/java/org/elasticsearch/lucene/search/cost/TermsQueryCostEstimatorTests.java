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
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class TermsQueryCostEstimatorTests extends ESTestCase {

    public void testConstructorRejectsNegativeArguments() {
        expectThrows(IllegalArgumentException.class, () -> new TermsQueryCostEstimator(-1));
    }

    public void testEstimateIsAtLeastTheFloor() {
        long zero = new TermsQueryCostEstimator(0).estimate();
        assertThat(zero, greaterThanOrEqualTo(TermsQueryCostEstimator.BASE_BYTES));
    }

    public void testEstimateIsMonotonicInTermsRamBytes() {
        long small = new TermsQueryCostEstimator(1024).estimate();
        long large = new TermsQueryCostEstimator(1024 * 1024).estimate();
        assertTrue("estimate must grow with the terms blob size", small < large);
    }

    public void testEstimateSaturatesOnOverflow() {
        assertEquals(Long.MAX_VALUE, new TermsQueryCostEstimator(Long.MAX_VALUE).estimate());
    }

    public void testEstimateCoversActualTermInSetQuerySize() {
        List<BytesRef> terms = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            terms.add(new BytesRef("term-" + i));
        }
        TermInSetQuery query = new TermInSetQuery("field", terms);
        long estimated = new TermsQueryCostEstimator(query.ramBytesUsed()).estimate();
        assertThat(
            "the estimate must be a ceiling on the actual TermInSetQuery ramBytesUsed()",
            estimated,
            greaterThanOrEqualTo(query.ramBytesUsed())
        );
    }

    public void testExecutionBytesForEmptyLeafIsZero() {
        assertEquals(0L, TermsQueryCostEstimator.executionBytesForLeaf(0, 0));
        assertEquals(0L, TermsQueryCostEstimator.executionBytesForLeaf(100, -1));
    }

    public void testExecutionBytesIsPositiveForNonEmptyLeaf() {
        assertThat(TermsQueryCostEstimator.executionBytesForLeaf(1, 1000000), greaterThan(0L));
    }

    public void testExecutionBytesSelectiveIsCheaperThanDense() {
        int maxDoc = 1000000;
        long dense = TermsQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc);
        long selective = TermsQueryCostEstimator.executionBytesForLeaf(maxDoc / 1000, maxDoc);
        assertTrue("a selective terms match must be charged less than the dense worst case", selective < dense);
    }

    public void testExecutionBytesIsCappedByDenseBitset() {
        int maxDoc = 1000000;
        long dense = TermsQueryCostEstimator.executionBytesForLeaf(maxDoc, maxDoc);
        long hugeCost = TermsQueryCostEstimator.executionBytesForLeaf(Long.MAX_VALUE / 8, maxDoc);
        assertEquals(dense, hugeCost);
    }

    public void testExecutionBytesSaturatesOnOverflow() {
        assertEquals(Long.MAX_VALUE, TermsQueryCostEstimator.executionBytesForLeaf(Long.MAX_VALUE, Integer.MAX_VALUE));
    }

    public void testExecutionBytesForLeafIsCeilingOnMeasuredRam() throws IOException {
        for (int maxDoc : new int[] { 1, 100, 10000, 1000000 }) {
            for (int matches : new int[] { 1, Math.max(1, maxDoc / 1000), Math.max(1, maxDoc / 2), maxDoc }) {
                long measured = docIdSetRamBytes(maxDoc, matches);
                long estimated = TermsQueryCostEstimator.executionBytesForLeaf(matches, maxDoc);
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
