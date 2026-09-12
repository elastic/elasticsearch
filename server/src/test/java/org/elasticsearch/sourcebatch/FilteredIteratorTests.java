/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.sourcebatch.LuceneColumn.FilteredIterator;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

public class FilteredIteratorTests extends ESTestCase {

    private static FixedBitSet bitset(int length, int... setBits) {
        FixedBitSet bs = new FixedBitSet(length);
        for (int bit : setBits) {
            bs.set(bit);
        }
        return bs;
    }

    private static List<Integer> drain(FilteredIterator fi, int... innerDocs) {
        int[] idx = { 0 };
        IntSupplier advance = () -> idx[0] < innerDocs.length ? innerDocs[idx[0]++] : DocIdSetIterator.NO_MORE_DOCS;
        List<Integer> result = new ArrayList<>();
        int compact;
        while ((compact = fi.nextDoc(advance)) != DocIdSetIterator.NO_MORE_DOCS) {
            result.add(compact);
        }
        return result;
    }

    public void testPartialOverlap() {
        // filter = {1, 3}, inner = {0, 1, 2, 3, 4} → compact IDs 0 and 1
        FilteredIterator fi = new FilteredIterator(bitset(5, 1, 3));
        assertEquals(List.of(0, 1), drain(fi, 0, 1, 2, 3, 4));
    }

    public void testFilterExhaustedBeforeData() {
        // filter bits run out at position 2; inner still has docs 3 and 4 — must not emit them
        FilteredIterator fi = new FilteredIterator(bitset(5, 0, 2));
        assertEquals(List.of(0, 1), drain(fi, 0, 1, 2, 3, 4));
    }

    public void testDataExhaustedBeforeFilter() {
        // inner only reaches doc 1; filter has bits through 4 — must stop at inner exhaustion
        FilteredIterator fi = new FilteredIterator(bitset(5, 0, 1, 2, 3, 4));
        assertEquals(List.of(0, 1), drain(fi, 0, 1));
    }

    public void testNoOverlap() {
        // filter bits are at {3, 4}, but inner exhausts at 2 without ever reaching them
        FilteredIterator fi = new FilteredIterator(bitset(5, 3, 4));
        assertEquals(List.of(), drain(fi, 0, 1, 2));
    }

    public void testAllMatch() {
        FilteredIterator fi = new FilteredIterator(bitset(3, 0, 1, 2));
        assertEquals(List.of(0, 1, 2), drain(fi, 0, 1, 2));
    }

    public void testInnerSparserThanFilter() {
        // inner skips positions 1 and 2 entirely; compact IDs reflect filter rank
        FilteredIterator fi = new FilteredIterator(bitset(4, 0, 1, 2, 3));
        assertEquals(List.of(0, 3), drain(fi, 0, 3));
    }

    public void testEmptyInner() {
        FilteredIterator fi = new FilteredIterator(bitset(5, 1, 3));
        assertEquals(List.of(), drain(fi));
    }
}
