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

    /** The next value of the current document; call exactly {@link #valueCount()} times per document. */
    public abstract BytesRef nextValue() throws IOException;

    /**
     * The ordinal this value takes in the column being written, when the cursor already knows it, or
     * {@code -1} when it does not and {@link #nextValue()} has to be asked instead.
     *
     * <p>A merge whose inputs are the dictionaries the vocabulary was built from knows it: the value's
     * ordinal in its own segment maps to one in the merged column, which costs a table per segment rather
     * than resolving every value's bytes only to look them up again. Exactly one of this and
     * {@link #nextValue()} advances the cursor.
     */
    public int nextOrdinal() throws IOException {
        return -1;
    }
}
