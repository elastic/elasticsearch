/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;

/**
 * Accounts the heap a script's doc-values reader retains while it is live against a circuit breaker.
 *
 * <p>A Painless script that reads {@code doc['field']} opens a doc-values reader per leaf through
 * {@link LeafDocLookup}. For a field stored in binary doc values that reader lazily allocates a decompression buffer
 * (up to ~512 KB under the time-series doc-values format), and one such buffer is held for every field, leaf and clause
 * the request touches at once. Nothing bounds that fan-out, which can exhaust the heap.
 *
 * <p>{@link LeafDocLookup} calls {@link #account(long, String, LeafReaderContext)} once per reader it opens, passing
 * the reader's estimated decode-buffer size and the leaf it was opened against. The implementation accounts the memory
 * against the request circuit breaker (and may trip it, aborting the request) and releases it later.
 *
 * <p>{@link LeafDocLookup} itself has no close or end-of-life hook — it is created inside scorers and script factories
 * that Lucene drops without notice — so it cannot release what it accounted. Instead the leaf context passed to
 * {@link #account(long, String, LeafReaderContext)} lets the implementation attribute the memory to the leaf through
 * the per-leaf execution accounting of {@code ContextIndexSearcher}, which releases a leaf's memory as soon as the
 * searcher finishes collecting it (or, for callers that score outside the searcher loop such as ES|QL's Lucene
 * operators, when their scorer scope is dropped). When no such searcher drives the request the accounted memory simply
 * stays on the breaker until the search context closes and drains it in one shot (see
 * {@code SearchExecutionContext#releaseQueryConstructionMemory()}). Both models are conservative within a single leaf
 * — every reader a leaf's clauses opened is accounted at once, exactly the situation of a {@code filters} aggregation
 * that builds all clause scorers per leaf up front — but the per-leaf release stops the accounted memory from
 * accumulating across leaves that are processed sequentially.
 */
@FunctionalInterface
public interface ScriptFieldMemoryEstimator {

    /**
     * A no-op estimator for contexts without a request circuit breaker (for example index-time lookups or tests).
     */
    ScriptFieldMemoryEstimator NO_OP = (bytes, field, ctx) -> {};

    /**
     * Account {@code bytes} of retained decode-buffer memory for {@code field} against the request circuit breaker.
     *
     * @param bytes the estimated decode-buffer size the reader retains; always positive when called
     * @param field the field whose reader was opened, used to label the accounted memory on the breaker
     * @param ctx   the leaf the reader was opened against, so the memory can be released when the leaf is done
     * @throws org.elasticsearch.common.breaker.CircuitBreakingException if the accounting trips the breaker
     */
    void account(long bytes, String field, LeafReaderContext ctx);
}
