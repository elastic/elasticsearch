/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * A competitive DocIdSetIterator that examines the values of a secondary
 * sort field and tries to exclude documents with values outside a given
 * range, using DocValueSkippers on the primary sort field to advance rapidly
 * to the next block of values.
 */
class SecondarySortIterator extends DocIdSetIterator {

    final NumericDocValues values;

    final DocValuesSkipper valueSkipper;
    final DocValuesSkipper primaryFieldSkipper;
    final long minValue;
    final long maxValue;

    int docID = -1;
    boolean skipperMatch;
    int primaryFieldUpTo = -1;
    int valueFieldUpTo = -1;

    SecondarySortIterator(
        NumericDocValues values,
        DocValuesSkipper valueSkipper,
        DocValuesSkipper primaryFieldSkipper,
        long minValue,
        long maxValue
    ) {
        this.values = values;
        this.valueSkipper = valueSkipper;
        this.primaryFieldSkipper = primaryFieldSkipper;
        this.minValue = minValue;
        this.maxValue = maxValue;

        valueFieldUpTo = valueSkipper.maxDocID(0);
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
        target = values.advance(target);
        if (target == DocIdSetIterator.NO_MORE_DOCS) {
            return docID = target;
        }
        while (true) {
            if (target > valueFieldUpTo) {
                valueSkipper.advance(target);
                valueFieldUpTo = valueSkipper.maxDocID(0);
                long minValue = valueSkipper.minValue(0);
                long maxValue = valueSkipper.maxValue(0);
                if (minValue > this.maxValue || maxValue < this.minValue) {
                    // outside the desired range, skip forward
                    for (int level = 1; level < valueSkipper.numLevels(); level++) {
                        minValue = valueSkipper.minValue(level);
                        maxValue = valueSkipper.maxValue(level);
                        if (minValue > this.maxValue || maxValue < this.minValue) {
                            valueFieldUpTo = valueSkipper.maxDocID(level);
                        } else {
                            break;
                        }
                    }

                    int upTo = valueFieldUpTo;
                    if (maxValue < this.minValue) {
                        // We've moved past the end of the valid values in the secondary sort field
                        // for this primary value. Advance the primary skipper to find the starting point
                        // for the next primary value, where the secondary field values will have reset
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

                    target = values.advance(upTo + 1);
                    if (target == DocIdSetIterator.NO_MORE_DOCS) {
                        return docID = target;
                    }
                } else if (minValue >= this.minValue && maxValue <= this.maxValue) {
                    assert valueSkipper.docCount(0) == valueSkipper.maxDocID(0) - valueSkipper.minDocID(0) + 1;
                    skipperMatch = true;
                    return docID = target;
                }
            }

            long value = values.longValue();
            if (value < minValue && target > primaryFieldUpTo) {
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
                    target = values.advance(primaryFieldUpTo + 1);
                    if (target == DocIdSetIterator.NO_MORE_DOCS) {
                        return docID = target;
                    }
                } else {
                    target = values.nextDoc();
                    if (target == DocIdSetIterator.NO_MORE_DOCS) {
                        return docID = target;
                    }
                }
            } else if (value >= minValue && value <= maxValue) {
                return docID = target;
            } else {
                target = values.nextDoc();
                if (target == DocIdSetIterator.NO_MORE_DOCS) {
                    return docID = target;
                }
            }
        }
    }

    @Override
    public int docIDRunEnd() throws IOException {
        if (skipperMatch) {
            return valueFieldUpTo + 1;
        }
        return super.docIDRunEnd();
    }

    @Override
    public long cost() {
        return values.cost();
    }

}
