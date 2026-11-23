/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.indices.RunEndCachingBitDocIdSet.RunEndCachingBitSetIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class RunEndCachingBitDocIdSetTests extends ESTestCase {

    private static final int MAX_SCAN = RunEndCachingBitSetIterator.MAX_RUN_END_SCAN;
    // mirrors the package-private DenseConjunctionBulkScorer.WINDOW_SIZE in Lucene
    private static final int WINDOW_SIZE = 4096;

    public void testIteratorExposesBitSet() {
        FixedBitSet bits = new FixedBitSet(100);
        bits.set(10, 20);
        DocIdSetIterator it = new RunEndCachingBitDocIdSet(bits, 10).iterator();
        assertThat(it, instanceOf(BitSetIterator.class));
        assertThat(BitSetIterator.getFixedBitSetOrNull(it), sameInstance(bits));
        assertEquals(10, it.cost());
    }

    public void testDocIDRunEndScanIsBounded() {
        int n = 2 * MAX_SCAN + 5;
        FixedBitSet bits = new FixedBitSet(n);
        bits.set(0, n);
        BitSetIterator it = new RunEndCachingBitSetIterator(bits, n);
        assertEquals(0, it.nextDoc());
        assertEquals(MAX_SCAN, it.docIDRunEnd());
        assertEquals(10, it.advance(10));
        assertEquals(10 + MAX_SCAN, it.docIDRunEnd());
        assertEquals(2 * MAX_SCAN, it.advance(2 * MAX_SCAN));
        assertEquals(n, it.docIDRunEnd());
    }

    public void testDocIDRunEndAfterMovingBackwards() {
        FixedBitSet bits = new FixedBitSet(1_000);
        bits.set(0, 100);
        bits.set(101, 300);
        BitSetIterator it = new RunEndCachingBitSetIterator(bits, bits.cardinality());
        assertEquals(150, it.advance(150));
        assertEquals(300, it.docIDRunEnd());
        it.setDocId(50);
        assertEquals(100, it.docIDRunEnd());
    }

    public void testDocIDRunEndAfterSetDocIdOnModifiedBitSet() {
        FixedBitSet bits = new FixedBitSet(1_000);
        bits.set(0, 300);
        BitSetIterator it = new RunEndCachingBitSetIterator(bits, bits.cardinality());
        assertEquals(50, it.advance(50));
        assertEquals(300, it.docIDRunEnd());
        bits.clear(100);
        it.setDocId(it.docID());
        assertEquals(100, it.docIDRunEnd());
    }

    public void testRandomDocIDRunEnd() {
        for (int iter = 0; iter < 10; iter++) {
            int n = randomIntBetween(1, 4 * MAX_SCAN);
            BitSet bits = randomBoolean() ? new FixedBitSet(n) : new SparseFixedBitSet(n);
            int numRuns = randomIntBetween(1, 10);
            for (int run = 0; run < numRuns; run++) {
                int start = randomIntBetween(0, n - 1);
                int end = randomIntBetween(start, n);
                for (int d = start; d < end; d++) {
                    bits.set(d);
                }
            }
            BitSetIterator it = new RunEndCachingBitSetIterator(bits, n);
            for (int d = it.nextDoc(); d != NO_MORE_DOCS; d = nextDoc(it, d, n)) {
                assertTrue(bits.get(d));
                assertEquals(expectedDocIDRunEnd(bits, d), it.docIDRunEnd());
            }
        }
    }

    public void testLongRunIsScannedOnce() {
        int maxDoc = 1 << 20;
        ScanCountingBitSet bits = new ScanCountingBitSet(maxDoc);
        bits.in.set(0, maxDoc);
        scoreInWindows(new RunEndCachingBitSetIterator(bits, maxDoc), maxDoc);
        assertThat("scanned " + bits.scannedBits + " bits", bits.scannedBits, lessThanOrEqualTo(2L * maxDoc));
    }

    /**
     * Mimics DenseConjunctionBulkScorer when deleted docs or a sparse clause cap each window to {@link #WINDOW_SIZE}: one
     * {@link DocIdSetIterator#docIDRunEnd()} call per window.
     */
    private static void scoreInWindows(BitSetIterator it, int maxDoc) {
        for (int min = 0; min < maxDoc; min += WINDOW_SIZE) {
            int doc = it.docID() < min ? it.advance(min) : it.docID();
            if (doc == NO_MORE_DOCS) {
                break;
            }
            assertThat(it.docIDRunEnd(), greaterThan(doc));
        }
    }

    /**
     * Moves the iterator past {@code doc} the ways a scorer can: advance, a bulk {@link BitSetIterator#intoBitSet} or a backwards
     * {@link BitSetIterator#setDocId}.
     */
    private static int nextDoc(BitSetIterator it, int doc, int maxDoc) {
        try {
            switch (randomIntBetween(0, 9)) {
                case 0 -> {
                    int upTo = Math.min(maxDoc, doc + 1 + randomIntBetween(0, 5_000));
                    it.intoBitSet(upTo, new FixedBitSet(upTo - doc), doc);
                    return it.docID();
                }
                case 1 -> {
                    int target = randomIntBetween(Math.max(0, doc - 2_000), doc);
                    it.setDocId(target);
                    return it.advance(target);
                }
                default -> {
                    return it.advance(doc + 1 + randomIntBetween(0, 5_000));
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static int expectedDocIDRunEnd(BitSet bits, int doc) {
        int end = doc + 1;
        int len = doc + Math.min(bits.length() - doc, MAX_SCAN);
        while (end < len && bits.get(end)) {
            end++;
        }
        return end;
    }

    /**
     * A {@link BitSet} that counts how many bits {@link #nextClearBit(int, int)} scans.
     */
    private static class ScanCountingBitSet extends BitSet {
        final FixedBitSet in;
        long scannedBits;

        ScanCountingBitSet(int length) {
            in = new FixedBitSet(length);
        }

        @Override
        public int nextClearBit(int start, int upperBound) {
            int res = in.nextClearBit(start, upperBound);
            scannedBits += (res == NO_MORE_DOCS ? upperBound : res + 1) - start;
            return res;
        }

        @Override
        public void set(int i) {
            in.set(i);
        }

        @Override
        public boolean getAndSet(int i) {
            return in.getAndSet(i);
        }

        @Override
        public void clear(int i) {
            in.clear(i);
        }

        @Override
        public void clear(int startIndex, int endIndex) {
            in.clear(startIndex, endIndex);
        }

        @Override
        public int cardinality() {
            return in.cardinality();
        }

        @Override
        public int approximateCardinality() {
            return in.approximateCardinality();
        }

        @Override
        public int prevSetBit(int index) {
            return in.prevSetBit(index);
        }

        @Override
        public int nextSetBit(int start, int end) {
            return in.nextSetBit(start, end);
        }

        @Override
        public boolean get(int index) {
            return in.get(index);
        }

        @Override
        public int length() {
            return in.length();
        }

        @Override
        public long ramBytesUsed() {
            return in.ramBytesUsed();
        }
    }
}
