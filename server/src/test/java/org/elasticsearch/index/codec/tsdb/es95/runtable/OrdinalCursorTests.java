/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.elasticsearch.test.ESTestCase;

/**
 * Unit tests for {@link OrdinalCursor}, the single-document-per-run fallback positioner. Each
 * document is its own run, so {@code run()} equals the target document, {@code numRuns()} equals
 * {@code maxDoc}, and {@code startDoc(run)} equals {@code run}. Covers sequential scan, random
 * forward and backward seeks, direct positioning, and reset.
 */
public class OrdinalCursorTests extends ESTestCase {

    public void testSingleDoc() {
        final OrdinalCursor cursor = new OrdinalCursor(1);
        cursor.seekDoc(0);
        assertEquals(0, cursor.run());
        assertEquals(1, cursor.numRuns());
        assertEquals(0, cursor.startDoc(0));
    }

    public void testNumRunsEqualsMaxDoc() {
        final int maxDoc = randomIntBetween(1, 10_000);
        final OrdinalCursor cursor = new OrdinalCursor(maxDoc);
        assertEquals(maxDoc, cursor.numRuns());
    }

    public void testStartDocEqualsRun() {
        final int maxDoc = randomIntBetween(1, 1000);
        final OrdinalCursor cursor = new OrdinalCursor(maxDoc);
        for (int run = 0; run < maxDoc; run++) {
            assertEquals("run " + run, run, cursor.startDoc(run));
        }
    }

    public void testSeekDocSetsRunToDoc() {
        final int maxDoc = randomIntBetween(1, 1000);
        final OrdinalCursor cursor = new OrdinalCursor(maxDoc);
        for (int doc = 0; doc < maxDoc; doc++) {
            cursor.seekDoc(doc);
            assertEquals("doc " + doc, doc, cursor.run());
        }
    }

    public void testRandomForwardJumps() {
        final int maxDoc = randomIntBetween(2, 1000);
        final OrdinalCursor cursor = new OrdinalCursor(maxDoc);
        int doc = -1;
        while (true) {
            final int next = doc + 1 + randomIntBetween(0, (maxDoc - 1) / 2);
            if (next >= maxDoc) {
                break;
            }
            doc = next;
            cursor.seekDoc(doc);
            assertEquals("doc " + doc, doc, cursor.run());
        }
    }

    public void testBackwardSeek() {
        final int maxDoc = randomIntBetween(2, 1000);
        final OrdinalCursor cursor = new OrdinalCursor(maxDoc);
        cursor.seekDoc(maxDoc - 1);
        assertEquals(maxDoc - 1, cursor.run());
        for (int iter = 0; iter < 50; iter++) {
            final int doc = randomIntBetween(0, maxDoc - 1);
            cursor.seekDoc(doc);
            assertEquals("doc " + doc, doc, cursor.run());
        }
    }

    public void testPositionOn() {
        final int maxDoc = randomIntBetween(2, 1000);
        final OrdinalCursor cursor = new OrdinalCursor(maxDoc);
        final int target = randomIntBetween(0, maxDoc - 1);
        cursor.positionOn(target);
        assertEquals(target, cursor.run());
        assertEquals(target, cursor.startDoc(cursor.run()));
    }

    public void testResetReturnsRunZero() {
        final int maxDoc = randomIntBetween(2, 1000);
        final OrdinalCursor cursor = new OrdinalCursor(maxDoc);
        cursor.seekDoc(randomIntBetween(1, maxDoc - 1));
        cursor.reset();
        assertEquals(0, cursor.run());
        cursor.seekDoc(randomIntBetween(0, maxDoc - 1));
        // reset again and verify seekDoc still works
        cursor.reset();
        cursor.seekDoc(0);
        assertEquals(0, cursor.run());
    }

    public void testSeekPastMaxDocAsserts() {
        final OrdinalCursor cursor = new OrdinalCursor(5);
        expectThrows(AssertionError.class, () -> cursor.seekDoc(5));
        expectThrows(AssertionError.class, () -> cursor.seekDoc(6));
    }

    public void testImplementsCursor() {
        final OrdinalCursor cursor = new OrdinalCursor(1);
        assertTrue(cursor instanceof Cursor);
    }
}
