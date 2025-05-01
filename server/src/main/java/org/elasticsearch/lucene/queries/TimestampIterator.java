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

import java.io.IOException;

// Similar to timestamp two phase iterator, without two matching. Used by TimestampComparator.
final class TimestampIterator extends DocIdSetIterator {

    final NumericDocValues timestamps;

    final DocValuesSkipper timestampSkipper;
    final DocValuesSkipper primaryFieldSkipper;
    final long minTimestamp;
    final long maxTimestamp;

    int docID = -1;
    boolean skipperMatch;
    int primaryFieldUpTo = -1;
    int timestampFieldUpTo = -1;

    TimestampIterator(
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

        timestampFieldUpTo = timestampSkipper.maxDocID(0);
        primaryFieldUpTo = primaryFieldSkipper.maxDocID(0);
    }

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(docID + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        skipperMatch = false;
        target = timestamps.advance(target);
        if (target == DocIdSetIterator.NO_MORE_DOCS) {
            return docID = target;
        }
        while (true) {
            if (target > timestampFieldUpTo) {
                timestampSkipper.advance(target);
                timestampFieldUpTo = timestampSkipper.maxDocID(0);
                long minValue = timestampSkipper.minValue(0);
                long maxValue = timestampSkipper.maxValue(0);
                if (minValue > maxTimestamp || maxValue < minTimestamp) {
                    for (int level = 1; level < timestampSkipper.numLevels(); level++) {
                        minValue = timestampSkipper.minValue(level);
                        maxValue = timestampSkipper.maxValue(level);
                        if (minValue > maxTimestamp || maxValue < minTimestamp) {
                            timestampFieldUpTo = timestampSkipper.maxDocID(level);
                        } else {
                            break;
                        }
                    }

                    int upTo = timestampFieldUpTo;
                    if (maxValue < minTimestamp) {
                        primaryFieldSkipper.advance(target);
                        primaryFieldUpTo = primaryFieldSkipper.maxDocID(0);
                        if (primaryFieldSkipper.minValue(0) == primaryFieldSkipper.maxValue(0)) {
                            for (int level = 1; level < primaryFieldSkipper.numLevels(); level++) {
                                if (primaryFieldSkipper.minValue(level) == primaryFieldSkipper.maxValue(level)) {
                                    primaryFieldUpTo = primaryFieldSkipper.maxDocID(level);
                                } else {
                                    break;
                                }
                            }
                        }
                        if (primaryFieldUpTo > upTo) {
                            upTo = primaryFieldUpTo;
                        }
                    }

                    target = timestamps.advance(upTo + 1);
                    if (target == DocIdSetIterator.NO_MORE_DOCS) {
                        return docID = target;
                    }
                } else if (minValue >= minTimestamp && maxValue <= maxTimestamp) {
                    assert timestampSkipper.docCount(0) == timestampSkipper.maxDocID(0) - timestampSkipper.minDocID(0) + 1;
                    skipperMatch = true;
                    return docID = target;
                }
            }

            long value = timestamps.longValue();
            if (value < minTimestamp && target > primaryFieldUpTo) {
                primaryFieldSkipper.advance(target);
                primaryFieldUpTo = primaryFieldSkipper.maxDocID(0);
                if (primaryFieldSkipper.minValue(0) == primaryFieldSkipper.maxValue(0)) {
                    for (int level = 1; level < primaryFieldSkipper.numLevels(); level++) {
                        if (primaryFieldSkipper.minValue(level) == primaryFieldSkipper.maxValue(level)) {
                            primaryFieldUpTo = primaryFieldSkipper.maxDocID(level);
                        } else {
                            break;
                        }
                    }
                    target = timestamps.advance(primaryFieldUpTo + 1);
                    if (target == DocIdSetIterator.NO_MORE_DOCS) {
                        return docID = target;
                    }
                } else {
                    target = timestamps.nextDoc();
                    if (target == DocIdSetIterator.NO_MORE_DOCS) {
                        return docID = target;
                    }
                }
            } else if (value >= minTimestamp && value <= maxTimestamp) {
                return docID = target;
            } else {
                target = timestamps.nextDoc();
                if (target == DocIdSetIterator.NO_MORE_DOCS) {
                    return docID = target;
                }
            }
        }
    }

    @Override
    public int docIDRunEnd() throws IOException {
        if (skipperMatch) {
            return timestampFieldUpTo + 1;
        }
        return super.docIDRunEnd();
    }

    @Override
    public long cost() {
        return timestamps.cost();
    }

}
