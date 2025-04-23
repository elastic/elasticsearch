/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;

import java.io.IOException;

/**
 * Based on {@link org.apache.lucene.search.DocValuesRangeIterator} but modified for time series and logsdb use cases.
 */
final class TimestampIterator extends TwoPhaseIterator {

    private final RangeNoGapsApproximation approximation;
    private final TwoPhaseIterator innerTwoPhase;

    TimestampIterator(
        TwoPhaseIterator twoPhase,
        DocValuesSkipper timestampSkipper,
        DocValuesSkipper primaryFieldSkipper,
        long minTimestamp,
        long maxTimestamp
    ) {
        super(new RangeNoGapsApproximation(twoPhase.approximation(), timestampSkipper, primaryFieldSkipper, minTimestamp, maxTimestamp));
        this.approximation = (RangeNoGapsApproximation) approximation();
        this.innerTwoPhase = twoPhase;
    }

    static final class RangeNoGapsApproximation extends DocIdSetIterator {

        private final DocIdSetIterator innerApproximation;

        final DocValuesSkipper timestampSkipper;
        final DocValuesSkipper primaryFieldSkipper;
        final long minTimestamp;
        final long maxTimestamp;

        private int doc = -1;

        // Track a decision for all doc IDs between the current doc ID and upTo inclusive.
        Match match = Match.MAYBE;
        int upTo = -1;

        RangeNoGapsApproximation(
            DocIdSetIterator innerApproximation,
            DocValuesSkipper timestampSkipper,
            DocValuesSkipper primaryFieldSkipper,
            long minTimestamp,
            long maxTimestamp
        ) {
            this.innerApproximation = innerApproximation;
            this.timestampSkipper = timestampSkipper;
            this.primaryFieldSkipper = primaryFieldSkipper;
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(docID() + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            while (true) {
                if (target > upTo) {
                    timestampSkipper.advance(target);
                    // If target doesn't have a value and is between two blocks, it is possible that advance()
                    // moved to a block that doesn't contain `target`.
                    target = Math.max(target, timestampSkipper.minDocID(0));
                    if (target == NO_MORE_DOCS) {
                        return doc = NO_MORE_DOCS;
                    }
                    upTo = timestampSkipper.maxDocID(0);
                    match = match(0);

                    // If we have a YES or NO decision, see if we still have the same decision on a higher
                    // level (= on a wider range of doc IDs)
                    int nextLevel = 1;
                    while (match != Match.MAYBE && nextLevel < timestampSkipper.numLevels() && match == match(nextLevel)) {
                        upTo = timestampSkipper.maxDocID(nextLevel);
                        nextLevel++;
                    }
                }
                switch (match) {
                    case YES:
                        return doc = target;
                    case MAYBE:
                    case NO:
                        if (match == Match.NO) {
                            primaryFieldSkipper.advance(target);
                            int betterUptTo = -1;
                            for (int level = 0; level < primaryFieldSkipper.numLevels(); level++) {
                                if (primaryFieldSkipper.minValue(level) == primaryFieldSkipper.maxValue(level)) {
                                    betterUptTo = primaryFieldSkipper.maxDocID(level);
                                } else {
                                    break;
                                }
                            }
                            if (betterUptTo > upTo) {
                                upTo = betterUptTo;
                            }
                        }

                        if (upTo == DocIdSetIterator.NO_MORE_DOCS) {
                            return doc = NO_MORE_DOCS;
                        }
                        target = upTo + 1;
                        break;
                    default:
                        throw new AssertionError("Unknown enum constant: " + match);
                }
            }
        }

        @Override
        public long cost() {
            return innerApproximation.cost();
        }

        Match match(int level) {
            long minValue = timestampSkipper.minValue(level);
            long maxValue = timestampSkipper.maxValue(level);
            if (minValue > maxTimestamp || maxValue < minTimestamp) {
                return Match.NO;
            } else if (minValue >= minTimestamp && maxValue <= maxTimestamp) {
                return Match.YES;
            } else {
                return Match.MAYBE;
            }
        }

    }

    @Override
    public boolean matches() throws IOException {
        return switch (approximation.match) {
            case YES -> true;
            case MAYBE -> innerTwoPhase.matches();
            case NO -> throw new IllegalStateException("Unpositioned approximation");
        };
    }

    @Override
    public int docIDRunEnd() throws IOException {
        if (approximation.match == Match.YES) {
            return approximation.upTo + 1;
        }
        return super.docIDRunEnd();
    }

    @Override
    public float matchCost() {
        return innerTwoPhase.matchCost();
    }

    enum Match {
        /** None of the documents in the range match */
        NO,
        /** Document values need to be checked to verify matches */
        MAYBE,
        /** All docs in the range match */
        YES;
    }
}
