/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * One walk that works out everything a string column has to know before it can start writing:
 * how many documents have a slot, how many slots there are in total, how many are null, and
 * optionally a vocabulary survey of the non-null values.
 *
 * <p>A census that only counts calls {@link StringColumnValues#nullCount()} per document —
 * no value decoding, just the per-document null count that the cursor already knows. A census
 * that also surveys decodes every value slot via {@link StringColumnValues#nextValue()} and
 * {@link StringColumnValues#value()}, counting nulls from {@code value() == null}.
 *
 * <p>Call {@link #needed} first — when both totals and vocabulary are already known, no census
 * is required and no cursor should be opened.
 */
final class StringCensus {

    private final boolean doSurvey;
    private final Vocabulary.Surveyor surveyor;

    private int numDocsWithField = 0;
    private long numValues = 0;
    private long numNullSlots = 0;

    /**
     * Whether a census is needed at all. True when either the totals are unknown or a survey must
     * be run (the policy is enabled and no vocabulary was supplied by the caller).
     */
    static boolean needed(StringColumnValues.Totals recorded, Vocabulary.Terms known, DictionaryPolicy policy) {
        return recorded == null || (policy.enabled() && known == null);
    }

    /**
     * @param doSurvey whether to accumulate a vocabulary survey during the walk
     * @param policy   the dictionary policy governing the survey; consulted only when
     *                 {@code doSurvey} is true
     */
    StringCensus(boolean doSurvey, DictionaryPolicy policy) {
        this.doSurvey = doSurvey;
        this.surveyor = doSurvey ? Vocabulary.surveyor(policy) : null;
    }

    /**
     * Walks the cursor once, accumulating counts and optionally surveying values.
     * Must be called at most once.
     */
    void walk(StringColumnValues cursor) throws IOException {
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            numDocsWithField++;
            if (doSurvey) {
                final int count = cursor.valueCount();
                numValues += count;
                for (int i = 0; i < count; i++) {
                    cursor.nextValue();
                    final BytesRef value = cursor.value();
                    if (value == null) {
                        numNullSlots++;
                    } else {
                        surveyor.accept(value);
                    }
                }
            } else {
                numValues += cursor.valueCount();
                numNullSlots += cursor.nullCount();
            }
        }
    }

    /** The counts accumulated during the walk. */
    StringColumnValues.Totals totals() {
        return new StringColumnValues.Totals(numDocsWithField, numValues, numNullSlots);
    }

    /**
     * The surveyed vocabulary, or {@code null} when the census did not survey or found nothing
     * worth naming. Must be called after {@link #walk}.
     */
    Vocabulary.Terms terms() {
        return doSurvey ? surveyor.finish() : null;
    }
}
