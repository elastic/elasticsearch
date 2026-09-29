/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

/**
 * Marks a query whose {@code matches()} opens a decoder over a field's binary doc values and decompresses whole
 * blocks into heap arrays — the {@code Scanning*} term/range/prefix/automaton/regexp queries in this package, plus
 * {@link BinaryDocValuesLengthQuery} and the wildcard module's {@code BinaryDvConfirmedQuery}.
 * <p>
 * Every surviving clause allocates its own decoder, and every decoder allocates its own uncompressed block buffer on
 * first decode. That buffer is sized to the segment's largest block for the field — up to several hundred KB — not to
 * the tiny handful of bytes a generic query leaf occupies. {@link org.elasticsearch.search.internal.MaxClauseCountQueryVisitor}
 * uses this interface to charge that real cost instead of its default per-leaf floor, so a query with thousands of
 * these clauses (e.g. a per-term fallback over a doc-values-only field) is rejected with a circuit breaker exception
 * up front rather than allocating every decoder's buffer during search and exhausting the heap.
 */
public interface BinaryDocValuesScanCost {

    /**
     * Conservative fixed per-clause estimate: the largest uncompressed block size a TSDB binary-DV codec can produce
     * ({@code ES819Version3TSDBDocValuesFormat}'s opt-in "large block" variant, 512 KiB) plus the decoder's other
     * per-instance state — chiefly the {@code int[]} of per-document offsets within a block, sized to that variant's
     * block doc-count threshold (8096), plus clone/decompressor shallow overhead.
     * <p>
     * This does not depend on which codec or block-size variant a field's segments actually use: reading that requires
     * plumbing the per-field block size out of the doc-values producer, which is a follow-up. Until then this assumes
     * the larger of the known variants, so the charge is conservative rather than an under-estimate.
     */
    long PER_CLAUSE_DECODE_BYTES_ESTIMATE = 512L * 1024 + 32L * 1024;

    /**
     * @param segmentCount leaf count of the index being searched, as passed to fuzzy/point-range cost estimation;
     *                      unused by the default estimate but kept so a future per-field estimate (or a concurrency
     *                      factor) can use it without changing every caller.
     * @return the estimated peak heap bytes one surviving clause charges against the request circuit breaker.
     */
    default long estimateDecodeBytes(int segmentCount) {
        return PER_CLAUSE_DECODE_BYTES_ESTIMATE;
    }
}
