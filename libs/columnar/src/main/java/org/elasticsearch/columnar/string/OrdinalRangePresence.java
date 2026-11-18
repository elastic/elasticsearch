/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.DocValuesSkipper;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

/**
 * The documents that hold a value, without those of an interval whose ordinal bounds miss a range of ordinals.
 * An ordinal window reads the blocks of the slots between two documents to find the next one it holds, so the
 * span has to be closed to it: once it finds nothing in an interval it would decode every block of the intervals
 * after it that hold nothing either. This iterator steps over such a span, and ends a run of documents at the
 * end of its interval. Of the levels covering a document it takes the highest that misses.
 *
 * <p>It only hides documents that no slot in the range can belong to, so a window read through it finds the
 * slots it would have found. It is built only over a column with no escapes, where every value has an ordinal.
 *
 * <p>It supports forward traversal only. {@code advanceExact} and {@code ranks} throw, because positioning on a
 * document of a hidden interval would break the assumption that the cached interval was accepted.
 */
final class OrdinalRangePresence extends ColumnIterator {

    private final ColumnIterator presence;
    private final DocValuesSkipper skipper;
    private final long lowOrdinal;
    private final long highOrdinal;

    private int intervalLastDoc = -1;
    private int highestExcludedLevel = -1;

    /**
     * @param presence    the documents that hold a value
     * @param skipper     the bounds of the ordinals by interval
     * @param lowOrdinal  the smallest ordinal in the range
     * @param highOrdinal the largest ordinal in the range
     */
    OrdinalRangePresence(ColumnIterator presence, DocValuesSkipper skipper, long lowOrdinal, long highOrdinal) {
        this.presence = presence;
        this.skipper = skipper;
        this.lowOrdinal = lowOrdinal;
        this.highOrdinal = highOrdinal;
    }

    @Override
    public int rank() {
        return presence.rank();
    }

    @Override
    public boolean advanceExact(int target) {
        throw new UnsupportedOperationException("a window reads documents in order");
    }

    @Override
    public int docID() {
        return presence.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        final int doc = presence.nextDoc();
        return doc <= intervalLastDoc ? doc : skipNonMatchingIntervals(doc);
    }

    @Override
    public int advance(int target) throws IOException {
        final int doc = presence.advance(target);
        return doc <= intervalLastDoc ? doc : skipNonMatchingIntervals(doc);
    }

    @Override
    public int docIDRunEnd() throws IOException {
        final int runEnd = presence.docIDRunEnd();
        return intervalLastDoc == NO_MORE_DOCS ? runEnd : Math.min(runEnd, intervalLastDoc + 1);
    }

    @Override
    public long cost() {
        return presence.cost();
    }

    private int skipNonMatchingIntervals(int doc) throws IOException {
        while (doc != NO_MORE_DOCS) {
            if (intervalLastDoc < doc) {
                skipper.advance(doc);
                intervalLastDoc = skipper.maxDocID(0);
                if (intervalLastDoc == NO_MORE_DOCS) {
                    return doc;
                }
                evaluateIntervalBounds();
            }
            if (highestExcludedLevel < 0) {
                return doc;
            }
            doc = presence.advance(skipper.maxDocID(highestExcludedLevel) + 1);
        }
        return NO_MORE_DOCS;
    }

    private void evaluateIntervalBounds() {
        highestExcludedLevel = -1;
        for (int level = skipper.numLevels() - 1; level >= 0 && highestExcludedLevel < 0; level--) {
            if (boundsDisjointFromRange(skipper.minValue(level), skipper.maxValue(level))) {
                highestExcludedLevel = level;
            }
        }
    }

    private boolean boundsDisjointFromRange(long min, long max) {
        return max < lowOrdinal || min > highOrdinal;
    }
}
