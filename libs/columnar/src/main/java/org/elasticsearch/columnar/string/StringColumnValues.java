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
 * <p>A slot is either a value or a null. Only a document holding at least one non-null slot is written at
 * all, so a cursor never yields a document whose slots are all null.
 *
 * <p>Sibling of {@code NumericColumnValues}; the returned {@link BytesRef} is only valid until the next
 * call to {@link #nextValue()}, so a caller that needs to retain it must copy.
 */
public abstract class StringColumnValues extends DocIdSetIterator {

    /** The number of slots the current document holds, null slots included. */
    public abstract int valueCount();

    /**
     * How many of the current document's slots are null. Separate from {@link #nextValue()} so the column
     * writer's counting pass — which needs the total up front, because a {@code DirectMonotonic} table is
     * built against a known entry count — can get it without pulling every value through, which on merge
     * would decode every block twice.
     */
    public abstract int nullCount() throws IOException;

    /**
     * The next slot of the current document, or {@code null} for a null slot; call exactly
     * {@link #valueCount()} times per document.
     */
    public abstract BytesRef nextValue() throws IOException;
}
