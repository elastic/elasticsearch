/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;

/**
 * Marks a query whose {@code matches()} opens a decoder over a field's binary doc values and decompresses whole
 * blocks into heap arrays — the {@code Scanning*} queries in this package, {@link BinaryDocValuesLengthQuery}, and
 * the wildcard module's {@code BinaryDvConfirmedQuery}.
 * <p>
 * Each surviving clause allocates its own decoder and, on first decode, its own uncompressed block buffer — up to
 * several hundred KB, not the handful of bytes a generic query leaf occupies. {@link
 * org.elasticsearch.search.internal.MaxClauseCountQueryVisitor} uses this interface to charge that real cost instead
 * of its default per-leaf floor, so a query with thousands of these clauses is rejected up front instead of OOMing.
 */
public interface BinaryDocValuesScanCost {

    /**
     * Conservative fixed per-clause estimate: {@link ES819Version3TSDBDocValuesFormat}'s "large block" binary-DV
     * variant's uncompressed block size, plus the {@code int[]} of per-document block offsets every decoder holds.
     * <p>
     * Doesn't know which codec or block-size variant a field's segments actually use — that needs the per-field
     * block size from the doc-values producer, a follow-up — so this assumes the larger of the known variants.
     * ({@code ES95TSDBDocValuesFormat} defines the same two thresholds independently and currently agrees.)
     */
    long PER_CLAUSE_DECODE_BYTES_ESTIMATE = ES819Version3TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT + (long) Integer.BYTES
        * (ES819Version3TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT + 1);

    /**
     * @param segmentCount unused by the default estimate; kept so a future per-field or concurrency-scaled estimate
     *                      doesn't need to change every caller.
     * @return estimated peak heap bytes one surviving clause charges against the request circuit breaker.
     */
    default long estimateDecodeBytes(int segmentCount) {
        return PER_CLAUSE_DECODE_BYTES_ESTIMATE;
    }
}
