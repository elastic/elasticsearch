/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;

/**
 * Bridges the columnar bulk batch-mapping path (mapper package) to the engine without the engine
 * depending on the mapper package directly. A mapper-side accumulator (see {@code
 * org.elasticsearch.index.mapper.BatchMappingContext}) implements this interface, assembling Lucene
 * {@link Column}s into a {@link ColumnBatch} that the engine hands to {@code IndexWriter#addBatch}.
 *
 * <p>Some columns — {@code _seq_no}, {@code _primary_term}, {@code _version} — are only known once
 * the engine has assigned them, long after mapping. Those are registered as array-backed columns
 * during mapping (placeholder values), and the engine fills the real per-document values.
 */
public interface ColumnBatchProvider {

    /** The number of documents this provider's columns span. */
    int docCount();

    /** Sets the engine-assigned {@code _seq_no} for batch-local document {@code doc}. */
    void setSeqNo(int doc, long seqNo);

    /**
     * Sets the engine-assigned {@code _primary_term} for every document in the batch.
     */
    void fillPrimaryTerm(long primaryTerm);

    /** Sets the engine-assigned {@code _version} for batch-local document {@code doc}. */
    void setVersion(int doc, long version);

    /**
     * Assembles the accumulated columns into a {@link ColumnBatch} covering documents {@code [from, to)}.
     *
     * <p>First cut: only the full range {@code [0, docCount())} is supported; sub-range slicing (needed
     * if a sub-batch splits due to version-lock contention) is a follow-up.
     *
     * @throws UnsupportedOperationException if {@code [from, to)} is not the full range
     */
    ColumnBatch columnBatch(int from, int to);
}
