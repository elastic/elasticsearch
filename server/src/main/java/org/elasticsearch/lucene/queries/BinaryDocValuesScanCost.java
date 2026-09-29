/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;

/**
 * Marks a query whose {@code matches()} opens a decoder over a field's binary doc values and decompresses whole
 * blocks into heap arrays — the {@code Scanning*} queries in this package, {@link BinaryDocValuesLengthQuery}, and
 * the wildcard module's {@code BinaryDvConfirmedQuery}.
 * <p>
 * Each surviving clause allocates its own decoder and, on first decode, its own uncompressed block buffer — up to
 * several hundred KB, not the handful of bytes a generic query leaf occupies. {@link
 * org.elasticsearch.search.internal.MaxClauseCountQueryVisitor} uses this interface to charge an upper-bound estimate
 * of that cost instead of its default per-leaf floor, so a query with thousands of these clauses is rejected up front
 * instead of OOMing.
 */
public interface BinaryDocValuesScanCost {

    /**
     * Per-clause estimate for the "large block" binary-DV variant: its uncompressed block size, plus the
     * {@code int[]} of per-document block offsets every decoder holds.
     */
    long PER_CLAUSE_DECODE_BYTES_ESTIMATE = ES95TSDBDocValuesFormatFactory.BINARY_BLOCK_BYTES_LARGE + (long) Integer.BYTES
        * (ES95TSDBDocValuesFormatFactory.BINARY_BLOCK_COUNT_LARGE + 1);

    /**
     * Same, for the small-block variant — the TSDB default unless {@code index.use_time_series_doc_values_format_large_binary_block_size}
     * is set.
     */
    long PER_CLAUSE_DECODE_BYTES_ESTIMATE_SMALL_BLOCK = ES95TSDBDocValuesFormatFactory.BINARY_BLOCK_BYTES_SMALL + (long) Integer.BYTES
        * (ES95TSDBDocValuesFormatFactory.BINARY_BLOCK_COUNT_SMALL + 1);

    /**
     * @param segmentCount unused by the default estimate; kept so a future per-field or concurrency-scaled estimate
     *                      doesn't need to change every caller.
     * @param largeBinaryBlock whether the index is confirmed to use the large-block binary-DV variant, or unknown —
     *                          see {@link org.elasticsearch.search.internal.MaxClauseCountQueryVisitor#largeBinaryBlockOrDefault}.
     * @return estimated peak heap bytes one surviving clause charges against the request circuit breaker.
     */
    default long estimateDecodeBytes(int segmentCount, boolean largeBinaryBlock) {
        return largeBinaryBlock ? PER_CLAUSE_DECODE_BYTES_ESTIMATE : PER_CLAUSE_DECODE_BYTES_ESTIMATE_SMALL_BLOCK;
    }
}
