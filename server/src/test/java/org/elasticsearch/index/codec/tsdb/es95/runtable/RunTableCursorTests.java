/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.util.LongValues;
import org.elasticsearch.test.ESTestCase;

/**
 * Unit tests for {@link RunTableCursor}, the packed-{@code startDoc[]}-backed positioner used
 * by the run-table ordinal readers. Covers the four positioning tiers: the one-doc-at-a-time
 * sequential scan, the O(1) step across a run boundary, the O(log runs) forward binary search
 * on a large jump, and the backward reset. Uniform-length runs give a closed-form expected run
 * of {@code doc / runLength}; an explicit non-uniform table covers adjacent single-doc runs and
 * the boundary docs; the single-run degenerate maps every doc to run zero.
 */
public class RunTableCursorTests extends ESTestCase {

    public void testSingleRun() {
        final int maxDoc = randomIntBetween(1, 1000);
        final RunTableCursor cursor = new RunTableCursor(longValues(new long[] { 0 }), 1, maxDoc);
        for (int doc = 0; doc < maxDoc; doc++) {
            cursor.seekDoc(doc);
            assertEquals("doc " + doc, 0, cursor.run());
        }
    }

    public void testSequentialForward() {
        final int runLength = randomIntBetween(1, 20);
        final int numRuns = randomIntBetween(1, 200);
        final int maxDoc = runLength * numRuns;
        final RunTableCursor cursor = uniformCursor(runLength, numRuns);
        for (int doc = 0; doc < maxDoc; doc++) {
            cursor.seekDoc(doc);
            assertEquals("doc " + doc, doc / runLength, cursor.run());
        }
    }

    public void testRandomForwardJumps() {
        final int runLength = randomIntBetween(1, 20);
        final int numRuns = randomIntBetween(2, 200);
        final int maxDoc = runLength * numRuns;
        final RunTableCursor cursor = uniformCursor(runLength, numRuns);
        int doc = -1;
        while (true) {
            final int next = doc + 1 + randomIntBetween(0, maxDoc / 2);
            if (next >= maxDoc) {
                break;
            }
            doc = next;
            cursor.seekDoc(doc);
            assertEquals("doc " + doc, doc / runLength, cursor.run());
        }
    }

    public void testBackwardResetsAndFindsRun() {
        final int runLength = randomIntBetween(1, 20);
        final int numRuns = randomIntBetween(2, 200);
        final int maxDoc = runLength * numRuns;
        final RunTableCursor cursor = uniformCursor(runLength, numRuns);
        cursor.seekDoc(maxDoc - 1);
        assertEquals(numRuns - 1, cursor.run());
        for (int iter = 0; iter < 50; iter++) {
            final int doc = randomIntBetween(0, maxDoc - 1);
            cursor.seekDoc(doc);
            assertEquals("doc " + doc, doc / runLength, cursor.run());
        }
    }

    public void testBoundaryRuns() {
        // Adjacent single-doc runs (0, 1, 2) exercise the O(1) sequential step; the jump from doc 2 to 99
        // forces the binary search; the remaining checks pin the run boundaries.
        final long[] startDocs = { 0, 1, 2, 100, 150 };
        final int maxDoc = 200;
        final RunTableCursor cursor = new RunTableCursor(longValues(startDocs), startDocs.length, maxDoc);
        assertRun(cursor, 0, 0);
        assertRun(cursor, 1, 1);
        assertRun(cursor, 2, 2);
        assertRun(cursor, 99, 2);
        assertRun(cursor, 100, 3);
        assertRun(cursor, 149, 3);
        assertRun(cursor, 150, 4);
        assertRun(cursor, 199, 4);
    }

    public void testSeekPastMaxDocAsserts() {
        final RunTableCursor cursor = new RunTableCursor(longValues(new long[] { 0 }), 1, 5);
        expectThrows(AssertionError.class, () -> cursor.seekDoc(5));
        expectThrows(AssertionError.class, () -> cursor.seekDoc(6));
    }

    public void testNumRuns() {
        final int numRuns = randomIntBetween(1, 200);
        final long[] startDocs = new long[numRuns];
        for (int i = 0; i < numRuns; i++) {
            startDocs[i] = i;
        }
        final RunTableCursor cursor = new RunTableCursor(longValues(startDocs), numRuns, numRuns);
        assertEquals(numRuns, cursor.numRuns());
    }

    public void testStartDocReturnsRunBoundary() {
        final long[] startDocs = { 0, 10, 25, 40 };
        final RunTableCursor cursor = new RunTableCursor(longValues(startDocs), startDocs.length, 50);
        for (int run = 0; run < startDocs.length; run++) {
            assertEquals("run " + run, (int) startDocs[run], cursor.startDoc(run));
        }
    }

    public void testPositionOnSkipsDirectly() {
        final int runLength = randomIntBetween(2, 20);
        final int numRuns = randomIntBetween(3, 100);
        final RunTableCursor cursor = uniformCursor(runLength, numRuns);
        cursor.seekDoc(0);
        final int targetRun = randomIntBetween(1, numRuns - 1);
        cursor.positionOn(targetRun);
        assertEquals(targetRun, cursor.run());
        assertEquals(targetRun * runLength, cursor.startDoc(targetRun));
    }

    public void testResetAfterSeekReturnsRunZero() {
        final int runLength = randomIntBetween(1, 20);
        final int numRuns = randomIntBetween(2, 200);
        final RunTableCursor cursor = uniformCursor(runLength, numRuns);
        cursor.seekDoc((numRuns - 1) * runLength);
        assertEquals(numRuns - 1, cursor.run());
        cursor.reset();
        assertEquals(0, cursor.run());
        cursor.seekDoc(runLength);
        assertEquals(1, cursor.run());
    }

    public void testImplementsCursor() {
        final RunTableCursor cursor = uniformCursor(1, 1);
        assertTrue(cursor instanceof Cursor);
    }

    private static void assertRun(final RunTableCursor cursor, int target, int expectedRun) {
        cursor.seekDoc(target);
        assertEquals("target " + target, expectedRun, cursor.run());
    }

    private static RunTableCursor uniformCursor(int runLength, int numRuns) {
        final long[] startDocs = new long[numRuns];
        for (int run = 0; run < numRuns; run++) {
            startDocs[run] = (long) run * runLength;
        }
        return new RunTableCursor(longValues(startDocs), numRuns, runLength * numRuns);
    }

    static LongValues longValues(final long[] values) {
        return new LongValues() {
            @Override
            public long get(long index) {
                return values[(int) index];
            }
        };
    }
}
