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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;

import java.io.IOException;

/**
 * Based on {@link org.apache.lucene.search.DocValuesRangeIterator} but modified for time series and logsdb use cases.
 * <p>
 * The @timestamp field always has exactly one value and all documents always have a @timestamp value.
 * Additionally, the @timestamp field is always the secondary index sort field and sort order is always descending.
 * <p>
 * This makes the doc value skipper on @timestamp field less effective, and so the doc value skipper for the first index sort field is
 * also used to skip to the next primary sort value if for the current skipper represents one value (min and max value are the same) and
 * the current value is lower than minTimestamp.
 */
final class TimestampTwoPhaseIterator extends TwoPhaseIterator {

    private final RangeNoGapsApproximation approximation;

    private final long minTimestamp;
    private final long maxTimestamp;

    TimestampTwoPhaseIterator(
        NumericDocValues timestamps,
        DocValuesSkipper timestampSkipper,
        DocValuesSkipper primaryFieldSkipper,
        long minTimestamp,
        long maxTimestamp
    ) {
        super(new RangeNoGapsApproximation(timestamps, timestampSkipper, primaryFieldSkipper, minTimestamp, maxTimestamp));
        this.approximation = (RangeNoGapsApproximation) approximation();
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    static final class RangeNoGapsApproximation extends DocIdSetIterator {

        private final NumericDocValues timestamps;

        final DocValuesSkipper timestampSkipper;
        final DocValuesSkipper primaryFieldSkipper;
        final long minTimestamp;
        final long maxTimestamp;

        private int doc = -1;

        // Track a decision for all doc IDs between the current doc ID and upTo inclusive.
        Match match = Match.MAYBE;
        int upTo = -1;
        int primaryFieldUpTo = -1;

        RangeNoGapsApproximation(
            NumericDocValues timestamps,
            DocValuesSkipper timestampSkipper,
            DocValuesSkipper primaryFieldSkipper,
            long minTimestamp,
            long maxTimestamp
        ) {
            this.timestamps = timestamps;
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
                    upTo = timestampSkipper.maxDocID(0);
                    if (upTo == DocIdSetIterator.NO_MORE_DOCS) {
                        return doc = DocIdSetIterator.NO_MORE_DOCS;
                    }
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
                        if (target > timestamps.docID()) {
                            target = timestamps.advance(target);
                        }
                        if (target <= upTo) {
                            if (target > primaryFieldUpTo) {
                                if (target > primaryFieldSkipper.maxDocID(0)) {
                                    primaryFieldSkipper.advance(target);
                                }
                                for (int level = 0; level < primaryFieldSkipper.numLevels(); level++) {
                                    if (primaryFieldSkipper.minValue(level) == primaryFieldSkipper.maxValue(level)) {
                                        primaryFieldUpTo = primaryFieldSkipper.maxDocID(level);
                                        match = Match.MAYBE_ONE_PRIMARY_SORT;
                                    } else {
                                        break;
                                    }
                                }
                            }

                            return doc = target;
                        }
                        break;
                    case NO_AND_SKIP:
                        if (target > primaryFieldUpTo) {
                            primaryFieldSkipper.advance(target);
                            for (int level = 0; level < primaryFieldSkipper.numLevels(); level++) {
                                if (primaryFieldSkipper.minValue(level) == primaryFieldSkipper.maxValue(level)) {
                                    primaryFieldUpTo = primaryFieldSkipper.maxDocID(level);
                                } else {
                                    break;
                                }
                            }
                            if (primaryFieldUpTo > upTo) {
                                upTo = primaryFieldUpTo;
                            }
                        }
                        if (upTo == DocIdSetIterator.NO_MORE_DOCS) {
                            return doc = NO_MORE_DOCS;
                        }
                        target = upTo + 1;
                        break;
                    case NO:
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
            return timestamps.cost();
        }

        Match match(int level) {
            long minValue = timestampSkipper.minValue(level);
            long maxValue = timestampSkipper.maxValue(level);
            if (minValue > maxTimestamp) {
                return Match.NO;
            } else if (maxValue < minTimestamp) {
                return Match.NO_AND_SKIP;
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
            case MAYBE -> {
                final long value = approximation.timestamps.longValue();
                yield value >= minTimestamp && value <= maxTimestamp;
            }
            case MAYBE_ONE_PRIMARY_SORT -> {
                final long value = approximation.timestamps.longValue();
                if (value < minTimestamp) {
                    approximation.match = Match.NO;
                    approximation.upTo = approximation.primaryFieldUpTo;
                    yield false;
                } else {
                    yield value <= maxTimestamp;
                }
            }
            case NO_AND_SKIP, NO -> throw new IllegalStateException("Unpositioned approximation");
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
        return 2; // 2 comparisons
    }

    enum Match {
        /** None of the documents in the range match */
        NO,
        /** Same as NO, but can maybe also skip to next primary sort value */
        NO_AND_SKIP,
        /** Document values need to be checked to verify matches */
        MAYBE,
        /** Same as {@link #MAYBE}, but only if there is one primary sort value in current skip entry  */
        MAYBE_ONE_PRIMARY_SORT,
        /** All docs in the range match */
        YES;
    }
}
