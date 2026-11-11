/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.document;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Objects;

/**
 * Fork of Lucene 10.5.1's package-private {@code RangeBulkScorer} with a fix for empty scoring
 * windows ({@code apache/lucene#16546}).
 *
 * <p>Delete this class and revert {@link XSortedSkipperScorerSupplier} once Elasticsearch upgrades
 * to a Lucene release containing that fix.
 */
final class XRangeBulkScorer extends BulkScorer {
    private final int minDocID;
    private final int maxDocID;
    private final Scorable scorer;
    private final DocIdSetIterator iterator;

    XRangeBulkScorer(DocIdSetIterator iterator, float score, int minDocID, int maxDocID) {
        if (minDocID >= maxDocID) {
            throw new IllegalArgumentException("minDocID must be less than maxDocID");
        }
        this.minDocID = minDocID;
        this.maxDocID = maxDocID;
        this.iterator = Objects.requireNonNull(iterator);
        this.scorer = new Scorable() {
            @Override
            public float score() {
                return score;
            }
        };
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        collector.setScorer(scorer);
        DocIdSetIterator competitiveIterator = collector.competitiveIterator();
        if (competitiveIterator != null) {
            if (competitiveIterator.docID() > min) {
                min = competitiveIterator.docID();
                min = Math.min(min, max);
            }
        }
        if (max <= minDocID) {
            iterator.advance(minDocID);
        } else if (min >= maxDocID) {
            iterator.advance(maxDocID);
        } else {
            int filteredMin = Math.max(min, minDocID);
            final int filteredMax = Math.min(max, maxDocID);
            iterator.advance(filteredMin);
            if (acceptDocs == null) {
                if (filteredMin < filteredMax) {
                    collector.collectRange(filteredMin, filteredMax);
                }
            } else {
                int rangeStart = -1;
                for (int doc = filteredMin; doc < filteredMax; doc++) {
                    if (acceptDocs.get(doc)) {
                        if (rangeStart < 0) {
                            rangeStart = doc;
                        }
                    } else if (rangeStart >= 0) {
                        collector.collectRange(rangeStart, doc);
                        rangeStart = -1;
                    }
                }
                if (rangeStart >= 0) {
                    collector.collectRange(rangeStart, filteredMax);
                }
            }
            iterator.advance(filteredMax);
        }
        return iterator.docID();
    }

    @Override
    public long cost() {
        return maxDocID - minDocID;
    }
}
