/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.test.ESTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class ScoringQueryTests extends ESTestCase {

    private static ScoringQuery.OffHeapCopy newCopy() {
        return new ScoringQuery.OffHeapCopy(n -> Arena.ofAuto().allocate(n));
    }

    private static byte[] contents(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    public void testFirstUseCopies() {
        ScoringQuery query = new ScoringQuery(new byte[] { 1, 2, 3 });
        assertArrayEquals(new byte[] { 1, 2, 3 }, contents(newCopy().segmentFor(query)));
    }

    public void testSameQueryKeepsCopy() {
        ScoringQuery query = new ScoringQuery(new byte[] { 1, 2, 3 });
        ScoringQuery.OffHeapCopy copy = newCopy();
        MemorySegment first = copy.segmentFor(query);
        assertSame(first, copy.segmentFor(query));
        assertSame(first, copy.segmentFor(query));
    }

    public void testRevisitedQueryGetsItsBytesBack() {
        ScoringQuery a = new ScoringQuery(new byte[] { 1, 2, 3 });
        ScoringQuery b = new ScoringQuery(new byte[] { 4, 5, 6 });
        ScoringQuery.OffHeapCopy copy = newCopy();
        assertArrayEquals(new byte[] { 1, 2, 3 }, contents(copy.segmentFor(a)));
        assertArrayEquals(new byte[] { 4, 5, 6 }, contents(copy.segmentFor(b)));
        assertArrayEquals(new byte[] { 1, 2, 3 }, contents(copy.segmentFor(a)));
    }

    public void testRecycledArrayInNewQueryGivesNewContents() {
        byte[] recycled = new byte[3];
        ScoringQuery.OffHeapCopy copy = newCopy();
        for (int cycle = 0; cycle < 10; cycle++) {
            byte b = (byte) cycle;
            recycled[0] = b;
            recycled[1] = b;
            recycled[2] = b;
            ScoringQuery query = new ScoringQuery(recycled);
            MemorySegment segment = copy.segmentFor(query);
            assertArrayEquals(new byte[] { b, b, b }, contents(segment));
            assertSame(segment, copy.segmentFor(query));
        }
    }

    public void testCopiesSharingQueriesStayCorrect() {
        ScoringQuery a = new ScoringQuery(new byte[] { 1, 2, 3 });
        ScoringQuery b = new ScoringQuery(new byte[] { 4, 5, 6 });
        ScoringQuery.OffHeapCopy first = newCopy();
        ScoringQuery.OffHeapCopy second = newCopy();
        MemorySegment firstSegment = first.segmentFor(a);
        MemorySegment secondSegment = second.segmentFor(a);
        for (int i = 0; i < 5; i++) {
            assertSame(firstSegment, first.segmentFor(a));
            assertSame(secondSegment, second.segmentFor(a));
        }
        for (int i = 0; i < 5; i++) {
            assertArrayEquals(new byte[] { 4, 5, 6 }, contents(first.segmentFor(b)));
            assertArrayEquals(new byte[] { 1, 2, 3 }, contents(second.segmentFor(a)));
            assertArrayEquals(new byte[] { 1, 2, 3 }, contents(first.segmentFor(a)));
            assertArrayEquals(new byte[] { 4, 5, 6 }, contents(second.segmentFor(b)));
        }
    }

    public void testFailedCopyIsRetried() {
        boolean[] failed = new boolean[1];
        ScoringQuery.OffHeapCopy copy = new ScoringQuery.OffHeapCopy(n -> {
            if (failed[0] == false) {
                failed[0] = true;
                throw new IllegalStateException("allocation failed");
            }
            return Arena.ofAuto().allocate(n);
        });
        ScoringQuery query = new ScoringQuery(new byte[] { 1, 2, 3 });
        expectThrows(IllegalStateException.class, () -> copy.segmentFor(query));
        assertArrayEquals(new byte[] { 1, 2, 3 }, contents(copy.segmentFor(query)));
    }

    public void testWritingQueryInUseFailsAssertion() {
        byte[] array = new byte[] { 1, 2, 3 };
        ScoringQuery query = new ScoringQuery(array);
        ScoringQuery.OffHeapCopy copy = newCopy();
        copy.segmentFor(query);
        array[0] = 9;
        expectThrows(AssertionError.class, () -> copy.segmentFor(query));
    }
}
