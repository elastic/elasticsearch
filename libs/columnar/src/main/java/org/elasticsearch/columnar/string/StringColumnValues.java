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
 * A forward, streaming cursor over a string column's slots on the write path — the input the column
 * writer pulls from. It iterates the documents that have a slot ({@link DocIdSetIterator}) and, for the
 * current document, yields its slots in written order. Nothing is materialized: on ingest it decodes one
 * payload at a time, and on merge it reads one block at a time off the mapped data input.
 *
 * <p>A slot is either a value or a null, and a document may hold nothing but nulls: the mapper writes a
 * payload for an all-null array, which is what keeps it distinct from a field that is absent, so a cursor
 * does yield such a document. What it never yields is a document with no slot at all.
 *
 * <p>Sibling of {@code NumericColumnValues}; the returned {@link BytesRef} is only valid until the next
 * call to {@link #nextValue()}, so a caller that needs to retain it must copy.
 */
public abstract class StringColumnValues extends DocIdSetIterator {

    /** The number of slots the current document holds, null slots included. */
    public abstract int valueCount();

    /**
     * How many of the current document's slots are null. Separate from the cursor so the column writer's
     * counting pass — which needs the total up front, because a {@code DirectMonotonic} table is built
     * against a known entry count — can get it without pulling every value through, which on merge would
     * decode every block twice.
     */
    public abstract int nullCount() throws IOException;

    /**
     * The totals the counting pass exists to collect, when this cursor can report them without being walked,
     * and null when it cannot. A merge reading columns this format wrote already has them: each segment
     * recorded its own, so the pass would walk the iterator and the addressing tables of every input only to
     * sum per-document counts back into a total that was on disk to begin with.
     *
     * <p>Only sound while nothing is dropped on the way through — a segment with deleted documents contributes
     * fewer than it recorded, and a cursor over one has to be counted.
     */
    public Totals totals() {
        return null;
    }

    /**
     * What a column is counted for: the documents holding at least one slot, the slots they hold between
     * them, and how many of those are null.
     */
    public record Totals(int numDocsWithField, long numValues, long numNullSlots) {}

    /**
     * Moves to the document's next slot; call exactly {@link #valueCount()} times per document. This is
     * the only thing that moves the cursor, so what a caller reads of a slot it reads as many times as it
     * likes and in whichever order.
     */
    public abstract void nextValue() throws IOException;

    /**
     * The value the cursor is on, or null when the slot is null — which is the only thing that says a slot is
     * null, so an implementation returning an empty {@link BytesRef} instead loses the distinction rather
     * than merely describing it differently.
     */
    public abstract BytesRef value() throws IOException;

    /**
     * The ordinal the value the cursor is on takes in the column being written, when the cursor already
     * knows it, or {@code -1} when it does not and {@link #value()} says what it is instead.
     *
     * <p>A merge whose inputs are the dictionaries the vocabulary was built from knows it: the value's
     * ordinal in its own segment maps to one in the merged column, which costs a table per segment rather
     * than resolving every value's bytes only to look them up again.
     */
    public int ordinal() throws IOException {
        return -1;
    }
}
