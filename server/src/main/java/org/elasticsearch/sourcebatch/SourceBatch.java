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

/**
 * An immutable, format-agnostic batch of source documents carrying a shared {@link SourceSchema}.
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
     * The raw serialized bytes of this batch. Used for wire transport and translog persistence.
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
     * Returns a view of this batch covering rows {@code [from, to)}.
     *
     * <p>The returned batch does not own the underlying buffers; closing it is a no-op.
     *
     * @throws IndexOutOfBoundsException if the range is invalid.
     */
    SourceBatch slice(int from, int to);
}
