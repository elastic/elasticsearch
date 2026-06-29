/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An immutable, format-agnostic batch of source documents carrying a shared {@link SourceSchema}.
 *
 * <p>Implementations may store documents in row-major (EIRF) or column-major layout. The schema,
 * type model ({@link org.elasticsearch.eirf.EirfType}), and compound-value readers
 * ({@link org.elasticsearch.eirf.EirfArrayReader} / {@link org.elasticsearch.eirf.EirfKeyValueReader})
 * are shared across all implementations.
 *
 * <p>This abstraction is intended to support both row-major and column-major access as first-class
 * modes, so callers can pick the one that fits the task independent of the physical layout. Today
 * only {@link #row(int)} (a single document's fields, needed for source reconstruction and
 * row-oriented consumers) is exposed; column-major access will be added alongside its first
 * consumer, when the column-major format lands and can drive the shape of that API.
 *
 * <p>Batches are {@link Releasable}; callers that own the batch must close it when done.
 * Slices produced by {@link #slice(int, int)} do not own the underlying buffers and their
 * {@link #close()} is a no-op.
 */
public interface SourceBatch extends Releasable, Accountable {

    /** The number of documents in this batch. */
    int docCount();

    /** The schema shared by every row in this batch. */
    SourceSchema schema();

    /** The number of leaf columns in the schema ({@code schema().leafCount()}). */
    int columnCount();

    /**
     * The raw serialised bytes of this batch. Used for wire transport and translog persistence.
     * The format of the bytes is implementation-defined.
     */
    BytesReference data();

    /**
     * Returns a reader for row {@code docIndex}.
     *
     * @throws IndexOutOfBoundsException if {@code docIndex} is out of {@code [0, docCount())}.
     */
    SourceRow row(int docIndex);

    /**
     * Returns an {@link Iterable} over all rows in order. Convenience wrapper around
     * {@link #row(int)}.
     */
    default Iterable<SourceRow> rows() {
        return () -> new Iterator<>() {
            private int next = 0;

            @Override
            public boolean hasNext() {
                return next < docCount();
            }

            @Override
            public SourceRow next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return row(next++);
            }
        };
    }

    /**
     * Returns a view of this batch covering rows {@code [from, to)}.
     *
     * <p>The returned batch does not own the underlying buffers; closing it is a no-op.
     *
     * @throws IndexOutOfBoundsException if the range is invalid.
     */
    SourceBatch slice(int from, int to);
}
