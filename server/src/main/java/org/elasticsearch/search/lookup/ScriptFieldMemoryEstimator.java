/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

/**
 * Accounts the heap a script's doc-values reader retains while it is live against a circuit breaker.
 *
 * <p>A Painless script that reads {@code doc['field']} opens a doc-values reader per leaf through
 * {@link LeafDocLookup}. For a field stored in binary doc values that reader lazily allocates a decompression buffer
 * (up to ~512 KB under the time-series doc-values format), and one such buffer is held for every field, leaf and clause
 * the request touches at once. Nothing bounds that fan-out, which can exhaust the heap — see
 * <a href="https://github.com/elastic/incident-management/issues/3404">incident-management #3404</a>.
 *
 * <p>{@link LeafDocLookup} calls {@link #account(long, String)} once per reader it opens, passing the reader's estimated
 * decode-buffer size. The implementation accounts the memory against the request circuit breaker (and may trip it,
 * aborting the request), and releases it when the request ends.
 *
 * <p>Note that although memory is accounted per leaf reader, it is released per request rather than per leaf reader.
 * {@link LeafDocLookup} has no close or end-of-life hook: it is created inside scorers and script factories that
 * Lucene drops without notice, so there is no point where "this leaf is done" is visible to the lookup. The accounted
 * bytes therefore stay on the breaker until the search context closes and drains them in one shot (see
 * {@code SearchExecutionContext#releaseQueryConstructionMemory()}), even though the underlying buffers may become
 * unreachable earlier. This makes the accounting deliberately conservative: it models the worst case where every
 * reader the request opened is live at once — exactly the situation of a {@code filters} aggregation that builds all
 * clause scorers per leaf up front — and overestimates for execution that processes leaves or clauses sequentially.
 */
@FunctionalInterface
public interface ScriptFieldMemoryEstimator {

    /**
     * A no-op estimator for contexts without a request circuit breaker (for example index-time lookups or tests).
     */
    ScriptFieldMemoryEstimator NO_OP = (bytes, field) -> {};

    /**
     * Account {@code bytes} of retained decode-buffer memory for {@code field} against the request circuit breaker.
     *
     * @param bytes the estimated decode-buffer size the reader retains; always positive when called
     * @param field the field whose reader was opened, used to label the accounted memory on the breaker
     * @throws org.elasticsearch.common.breaker.CircuitBreakingException if the accounting trips the breaker
     */
    void account(long bytes, String field);
}
