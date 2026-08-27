/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.document;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.LongPredicate;

/**
 * Fork of Lucene 10.5.1's package-private {@code SortedSkipperScorerSupplier} wired to
 * {@link XRangeBulkScorer}.
 *
 * <p>Delete this class once Elasticsearch upgrades to a Lucene release containing the fix for
 * {@code apache/lucene#16546}.
 */
public abstract class XSortedSkipperScorerSupplier extends ScorerSupplier {

    private final DocValuesSkipper skipper;
    private final SortField sortField;
    private final ScoreMode scoreMode;
    private final float score;
    private int skipperMinDocId = -1;
    private int skipperMaxDocId = -1;
    private boolean skipperMinDocIdExact = false;
    private boolean skipperMaxDocIdExact = false;

    protected XSortedSkipperScorerSupplier(DocValuesSkipper skipper, SortField sortField, float score, ScoreMode scoreMode) {
        this.scoreMode = scoreMode;
        this.score = score;
        this.skipper = skipper;
        this.sortField = sortField;
    }

    protected abstract long getLowerValue() throws IOException;

    protected abstract long getUpperValue() throws IOException;

    protected abstract int nextDoc(int startDocID, LongPredicate predicate) throws IOException;

    @Override
    public final Scorer get(long leadCost) throws IOException {
        DocIdRange range = range();
        DocIdSetIterator iterator = range.minDocID() == range.maxDocID()
            ? DocIdSetIterator.empty()
            : DocIdSetIterator.range(range.minDocID(), range.maxDocID());
        return new ConstantScoreScorer(score, scoreMode, iterator);
    }

    @Override
    public final BulkScorer bulkScorer() throws IOException {
        DocIdRange range = range();
        if (range.minDocID() == range.maxDocID()) {
            return emptyBulkScorer();
        }
        DocIdSetIterator iterator = DocIdSetIterator.range(range.minDocID(), range.maxDocID());
        return new XRangeBulkScorer(iterator, score, range.minDocID(), range.maxDocID());
    }

    @Override
    public long cost() {
        if (skipperMinDocId == -1) {
            try {
                computeSkipperDocIds();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (skipperMaxDocIdExact) {
            return skipperMaxDocId - skipperMinDocId;
        }
        return skipperMaxDocId - skipperMinDocId + skipper.docCount(0);
    }

    private DocIdRange range() throws IOException {
        if (skipperMinDocId == -1) {
            computeSkipperDocIds();
        }
        long minOrd = getLowerValue();
        long maxOrd = getUpperValue();
        final int minDocID;
        final int maxDocID;
        if (sortField.getReverse()) {
            minDocID = skipperMinDocIdExact ? skipperMinDocId : nextDoc(skipperMinDocId, l -> l <= maxOrd);
            maxDocID = skipperMaxDocIdExact ? skipperMaxDocId : nextDoc(skipperMaxDocId, l -> l < minOrd);
        } else {
            minDocID = skipperMinDocIdExact ? skipperMinDocId : nextDoc(skipperMinDocId, l -> l >= minOrd);
            maxDocID = skipperMaxDocIdExact ? skipperMaxDocId : nextDoc(skipperMaxDocId, l -> l > maxOrd);
        }
        return new DocIdRange(minDocID, maxDocID);
    }

    private void computeSkipperDocIds() throws IOException {
        long minOrd = getLowerValue();
        long maxOrd = getUpperValue();
        if (minOrd > maxOrd || minOrd > skipper.maxValue() || maxOrd < skipper.minValue()) {
            skipperMinDocId = skipperMaxDocId = DocIdSetIterator.NO_MORE_DOCS;
            skipperMinDocIdExact = skipperMaxDocIdExact = true;
            return;
        }
        if (skipper.minValue() >= minOrd && skipper.maxValue() <= maxOrd) {
            skipperMinDocId = 0;
            skipperMaxDocId = skipper.docCount();
            skipperMinDocIdExact = skipperMaxDocIdExact = true;
            return;
        }
        if (sortField.getReverse()) {
            if (skipper.maxValue() <= maxOrd) {
                skipperMinDocId = 0;
                skipperMinDocIdExact = true;
            } else {
                skipper.advance(Long.MIN_VALUE, maxOrd);
                skipperMinDocId = skipper.minDocID(0);
                skipperMinDocIdExact = skipper.maxValue(0) == maxOrd;
            }
            if (skipper.minValue() >= minOrd) {
                skipperMaxDocId = skipper.docCount();
                skipperMaxDocIdExact = true;
            } else {
                skipper.advance(Long.MIN_VALUE, minOrd - 1);
                skipperMaxDocId = skipper.minDocID(0);
                skipperMaxDocIdExact = skipper.maxValue(0) < minOrd;
            }
        } else {
            if (skipper.minValue() >= minOrd) {
                skipperMinDocId = 0;
                skipperMinDocIdExact = true;
            } else {
                skipper.advance(minOrd, Long.MAX_VALUE);
                skipperMinDocId = skipper.minDocID(0);
                skipperMinDocIdExact = skipper.minValue(0) == minOrd;
            }
            if (skipper.maxValue() <= maxOrd) {
                skipperMaxDocId = skipper.docCount();
                skipperMaxDocIdExact = true;
            } else {
                skipper.advance(maxOrd + 1, Long.MAX_VALUE);
                skipperMaxDocId = skipper.minDocID(0);
                skipperMaxDocIdExact = skipper.minValue(0) > maxOrd;
            }
        }
    }

    private record DocIdRange(int minDocID, int maxDocID) {}

    private static BulkScorer emptyBulkScorer() {
        return new BulkScorer() {
            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }
}
