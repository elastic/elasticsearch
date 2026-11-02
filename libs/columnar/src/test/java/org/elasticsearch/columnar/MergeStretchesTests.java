/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.index.MergeState;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.test.ESTestCase;

/**
 * Where a segment's documents land next to one another is found by searching where nothing was deleted, rather than by
 * stepping through every document, and a chunk is copied into the place the search says. These hold it to what stepping
 * through answers, over segments interleaved at random in runs of every length.
 */
public class MergeStretchesTests extends ESTestCase {

    public void testSearchFindsWhatSteppingThroughFinds() {
        for (int iter = 0; iter < 200; iter++) {
            final int segments = between(2, 5);
            final int[] owner = new int[between(1, 3000)];
            // Each position of the merged segment goes to one of the segments, in runs of a random length.
            for (int at = 0; at < owner.length;) {
                final int segment = randomInt(segments - 1);
                final int run = randomBoolean() ? 1 : between(1, randomBoolean() ? 8 : 400);
                for (int i = 0; i < run && at < owner.length; i++) {
                    owner[at++] = segment;
                }
            }
            for (int segment = 0; segment < segments; segment++) {
                assertStretchesOf(owner, segment);
            }
        }
    }

    public void testASegmentAppendedWholeIsOneStretch() {
        final MergeStretches stretches = new MergeStretches(doc -> doc, null, between(1, 1000), false);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, stretches.contiguousEnd(0));
    }

    public void testOnlyAStretchLongEnoughToHoldAChunkIsOffered() {
        assertTrue(MergeStretches.worthOffering(5, DocIdSetIterator.NO_MORE_DOCS));
        assertTrue(MergeStretches.worthOffering(5, 5 + MergeStretches.MIN_OFFERED_DOCS));
        assertFalse(MergeStretches.worthOffering(5, 5 + MergeStretches.MIN_OFFERED_DOCS - 1));
        assertFalse(MergeStretches.worthOffering(5, 6));
    }

    /** The documents of {@code segment} take the positions {@code owner} gives it, in order, as an index sort lays them out. */
    private static void assertStretchesOf(int[] owner, int segment) {
        int count = 0;
        for (int position : owner) {
            if (position == segment) {
                count++;
            }
        }
        final int[] newIds = new int[count];
        for (int at = 0, doc = 0; at < owner.length; at++) {
            if (owner[at] == segment) {
                newIds[doc++] = at;
            }
        }
        final MergeStretches stretches = new MergeStretches((MergeState.DocMap) doc -> newIds[doc], null, count, true);
        // Asked in ascending order, from documents that start a stretch and from ones inside it.
        for (int doc = 0; doc < count; doc++) {
            if (doc > 0 && randomBoolean()) {
                continue;
            }
            int expected = doc + 1;
            while (expected < count && newIds[expected] == newIds[doc] + (expected - doc)) {
                expected++;
            }
            assertEquals(
                "segment " + segment + " document " + doc,
                expected == count ? DocIdSetIterator.NO_MORE_DOCS : expected,
                stretches.contiguousEnd(doc)
            );
        }
    }
}
