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
 * A forward, streaming cursor over a string column's values on the write path — the input the column
 * writer pulls from. It iterates the documents that have a value ({@link DocIdSetIterator}) and, for the
 * current document, yields its values in written order. Nothing is materialized: on ingest it decodes one
 * payload at a time, and on merge it reads one block at a time off the mapped data input.
 *
 * <p>Sibling of {@code NumericColumnValues}; the returned {@link BytesRef} is only valid until the next
 * call to {@link #nextValue()}, so a caller that needs to retain it must copy.
 */
public abstract class StringColumnValues extends DocIdSetIterator {

    /** The number of values the current document holds. */
    public abstract int valueCount();

    /**
     * Moves to the document's next value; call exactly {@link #valueCount()} times per document. This is
     * the only thing that moves the cursor, so what a caller reads of a value it reads as many times as it
     * likes and in whichever order.
     */
    public abstract void nextValue() throws IOException;

    /** The value the cursor is on. */
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
