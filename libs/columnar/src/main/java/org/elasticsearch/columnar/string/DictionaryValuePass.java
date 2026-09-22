/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * A {@link ColumnIteratorWriter.Pass} that writes a dictionary string column's per-slot data:
 * one vint ordinal per slot to the ordinal temp file, escaped values to the escape temp file, and escape
 * rank entries periodically to the rank writer. The pass accumulates whether ordinals arrived in term order
 * ({@link #sorted}), the running value address ({@link #index}), and the escape count ({@link #escapes}),
 * all read by the caller after the walk completes.
 *
 * <p>Care point for callers: {@link #finishRanks()} must be called after the walk ends and before
 * {@link MonotonicWriter#finish} — it writes the one-past-the-end escape rank entry that
 * {@link StringColumnWriter#escapeRankEntries} accounts for.
 */
final class DictionaryValuePass implements ColumnIteratorWriter.Pass<StringColumnValues> {

    private final Vocabulary.Terms vocabulary;
    private final int escapeOrdinal;
    private final int escapeRankBlockSize;
    private final MonotonicWriter ranks;
    private final AddressingWriter slots;
    private final IndexOutput ordinalTemp;
    private final IndexOutput escapeTemp;
    private final BytesRefBuilder previous = new BytesRefBuilder();

    // Accumulated state read by the caller after the walk.
    private long escapes = 0;
    private long index = 0;
    private boolean sorted = true;
    private int previousOrdinalSeen = -1;
    private int previousOrdinal = Vocabulary.DROPPED;
    private boolean hasPrevious = false;

    DictionaryValuePass(
        Vocabulary.Terms vocabulary,
        int escapeOrdinal,
        int escapeRankBlockSize,
        MonotonicWriter ranks,
        AddressingWriter slots,
        IndexOutput ordinalTemp,
        IndexOutput escapeTemp
    ) {
        this.vocabulary = vocabulary;
        this.escapeOrdinal = escapeOrdinal;
        this.escapeRankBlockSize = escapeRankBlockSize;
        this.ranks = ranks;
        this.slots = slots;
        this.ordinalTemp = ordinalTemp;
        this.escapeTemp = escapeTemp;
    }

    @Override
    public void accept(StringColumnValues cursor, int doc) throws IOException {
        slots.startDocument(index);
        for (int i = 0, count = cursor.valueCount(); i < count; i++) {
            if (index % escapeRankBlockSize == 0) {
                ranks.add(escapes);
            }
            cursor.nextValue();
            // A cursor that already knows the ordinal saves resolving the value's bytes only to look them
            // up again, which is most of what merging such a column costs. It answers for terms alone,
            // so a null still costs its bytes to recognise — which for a null is no bytes at all.
            final int mapped = cursor.ordinal();
            if (mapped >= 0) {
                // Carried over rather than resolved, but it still says where the value sits among the
                // terms, so the order is read from it as from any other ordinal.
                if (mapped < previousOrdinalSeen) {
                    sorted = false;
                }
                previousOrdinalSeen = mapped;
                ordinalTemp.writeVInt(mapped);
                index++;
                continue;
            }
            final BytesRef value = cursor.value();
            // A null is named by the reserved ordinal below the terms. It never reaches the dictionary or
            // the escapes, so it cannot be confused with the empty term, and a column whose only unnamed
            // values were nulls still reports no escapes.
            if (value == null) {
                // No place in term order, so a column holding one is not one to bisect.
                sorted = false;
                ordinalTemp.writeVInt(StringColumnMetadata.Dictionary.NULL_ORDINAL);
                index++;
                continue;
            }
            final int ordinal;
            if (hasPrevious && previous.get().bytesEquals(value)) {
                ordinal = previousOrdinal;
            } else {
                final int id = vocabulary.terms().find(value);
                // A term the survey saw can still have been dropped from the dictionary, so the ordinal is
                // shifted only once it is known to name one — DROPPED shifted would land on a reserved
                // ordinal rather than staying a marker.
                final int termOrdinal = id >= 0 ? vocabulary.ordinalOfId()[id] : Vocabulary.DROPPED;
                ordinal = termOrdinal == Vocabulary.DROPPED
                    ? Vocabulary.DROPPED
                    : termOrdinal + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
                previous.copyBytes(value);
                previousOrdinal = ordinal;
                hasPrevious = true;
            }
            if (ordinal == Vocabulary.DROPPED) {
                sorted = false;
                ordinalTemp.writeVInt(escapeOrdinal);
                escapeTemp.writeVInt(value.length);
                escapeTemp.writeBytes(value.bytes, value.offset, value.length);
                escapes++;
            } else {
                if (ordinal < previousOrdinalSeen) {
                    sorted = false;
                }
                previousOrdinalSeen = ordinal;
                ordinalTemp.writeVInt(ordinal);
            }
            index++;
        }
    }

    /**
     * Writes the one-past-the-end escape rank entry. Must be called after the walk ends and before
     * {@link MonotonicWriter#finish}.
     */
    void finishRanks() throws IOException {
        ranks.add(escapes);
    }

    /** Number of values that escaped the dictionary. */
    long escapes() {
        return escapes;
    }

    /** Total slots written across all documents visited. */
    long index() {
        return index;
    }

    /** Whether all values arrived in ascending ordinal order. */
    boolean sorted() {
        return sorted;
    }
}
