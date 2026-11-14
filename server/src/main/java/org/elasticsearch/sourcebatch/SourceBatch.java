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
import org.elasticsearch.plugins.internal.MeteringModel;

/**
 * An immutable, format-agnostic batch of source documents carrying a shared {@link SourceSchema}.
 *
 * <p>Batches are {@link Releasable}; callers that own the batch must close it exactly once when done.
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
     * Returns {@link SourceRow#sizeInBytes()} of every row, indexed by row, computed for the whole batch at
     * once. The default sizes the rows one at a time through {@link #row(int)}, which touches every column
     * for every document; a column-major implementation overrides it to touch each column once, which is
     * what the batch indexing path needs when it sizes every document of a batch.
     */
    default int[] rowSizesInBytes() {
        final int[] sizes = new int[docCount()];
        for (int d = 0; d < sizes.length; d++) {
            sizes[d] = row(d).sizeInBytes();
        }
        return sizes;
    }

    /**
     * Returns the size metered for every row under {@code model}, indexed by row: the model's charge for each value of the row,
     * leaving out the values below an excluded root field. Only a column-major batch meters its rows, in one
     * sweep over each column; there is no row-by-row form, so the default throws.
     */
    default long[] meteredRowSizes(MeteringModel model) {
        throw new UnsupportedOperationException("metering is only supported for columnar batches");
    }

    /**
     * Returns a view of this batch covering rows {@code [from, to)}.
     *
     * <p>The returned batch does not own the underlying buffers; closing it is a no-op.
     *
     * @throws IndexOutOfBoundsException if the range is invalid.
     */
    SourceBatch slice(int from, int to);

    /**
     * Estimates the bytes this batch contributes to the Lucene indexing buffer.
     */
    int estimatedBytes();

    /**
     * Returns {@code true} if every present row in column {@code columnIndex} is either absent, JSON
     * {@code null}, or an empty object ({@code {}}). When {@code true}, the column carries no indexable
     * value and can be safely ignored during batch mapper resolution.
     *
     * <p>The default implementation returns {@code false} (conservative: treat as non-empty). Format-aware
     * implementations override this to inspect column data directly.
     */
    default boolean isEmptyObjectColumn(int columnIndex) {
        return false;
    }
}
