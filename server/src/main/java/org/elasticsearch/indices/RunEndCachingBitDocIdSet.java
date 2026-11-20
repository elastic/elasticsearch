/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link DocIdSet} over a {@link FixedBitSet} whose iterator answers {@link DocIdSetIterator#docIDRunEnd()} without rescanning
 * the same run of set bits on every call.
 * <p>
 * Lucene 10.5's {@link BitSetIterator#docIDRunEnd()} scans to the next clear bit on every call, and
 * {@code DenseConjunctionBulkScorer} calls it once per 4096-doc window. A cached filter matching most of a large segment is then
 * rescanned on every window, which is quadratic in the segment size. This backports the fix from apache/lucene#16849 (Lucene 10.6.0)
 * to the dense doc id sets of the query cache only: uncached queries that produce a plain {@link BitSetIterator} (e.g. dense point
 * range or terms queries) are not covered. Remove once Elasticsearch is on a Lucene version that includes the fix.
 */
final class RunEndCachingBitDocIdSet extends DocIdSet {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RunEndCachingBitDocIdSet.class);

    private final FixedBitSet bits;
    private final long cost;

    RunEndCachingBitDocIdSet(FixedBitSet bits, long cost) {
        this.bits = bits;
        this.cost = cost;
    }

    @Override
    public DocIdSetIterator iterator() {
        return new RunEndCachingBitSetIterator(bits, cost);
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + bits.ramBytesUsed();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(set=" + bits + ",cost=" + cost + ")";
    }

    /**
     * Remains a {@link BitSetIterator} so that {@link BitSetIterator#getFixedBitSetOrNull} and the bulk paths keyed on it still apply.
     */
    static final class RunEndCachingBitSetIterator extends BitSetIterator {

        // 16 DenseConjunctionBulkScorer windows, so callers only consuming a prefix of a run (doc-partitioned slices, early
        // termination) don't scan all of it
        static final int MAX_RUN_END_SCAN = 1 << 16;

        private final BitSet bits;
        private final int length;
        private int runStart = -1;
        private int runEnd = -1;

        RunEndCachingBitSetIterator(BitSet bits, long cost) {
            super(bits, cost);
            this.bits = bits;
            this.length = bits.length();
        }

        @Override
        public void setDocId(int docId) {
            super.setDocId(docId);
            runStart = runEnd = -1;
        }

        @Override
        public int docIDRunEnd() {
            assert doc != NO_MORE_DOCS;
            if (doc < runStart || doc >= runEnd) {
                runStart = doc;
                runEnd = doc + 1;
            }
            int upTo = doc + Math.min(length - doc, MAX_RUN_END_SCAN);
            if (runEnd < upTo) {
                int end = bits.nextClearBit(runEnd, upTo);
                runEnd = end == NO_MORE_DOCS ? upTo : end;
            }
            return runEnd;
        }
    }
}
