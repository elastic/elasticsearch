/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.query.QueryProfiler;

/**
 *
 * <p> This interface includes the declaration of an abstract method, profile(). Classes implementing this interface
 * must provide an implementation for profile() to store profiling information in the {@link QueryProfiler}.
 */

public interface QueryProfilerProvider {

    /**
     * Store the profiling information in the {@link QueryProfiler}
     * @param queryProfiler an instance of  {@link KnnFloatVectorField}.
     */
    void profile(QueryProfiler queryProfiler);

    /**
     * Enables detailed profiling data collection for this query, allocating the timing breakdown state.
     * Must be called before {@code rewrite()} to take effect.
     * <p>
     * On the normal search path this is not called at all: a kNN query enables itself when it discovers a
     * {@link QueryProfiler} on the {@link ContextIndexSearcher} during {@code rewrite()}. This is the explicit
     * entry point for a caller that drives the query itself - tests running against a plain
     * {@code IndexSearcher}, and {@link PostFilterKnnQuery}, which profiles each of its per-round inner
     * searches in isolation.
     * <p>
     * Calling this transfers ownership of the breakdown to the caller: a query enabled this way collects but
     * never publishes itself to the profiler on the searcher, because whoever enabled it is expected to
     * harvest it through {@link #profile}. That is what keeps a post-filter round's inner breakdown nested
     * under {@code post_filter.rounds[]} instead of also appearing as a top-level {@code knn_profile} entry,
     * with its vector ops counted twice.
     */
    default void enableProfiling() {}

    /**
     * Records the vector quantization / index-options type (e.g. {@code bbq_hnsw}, {@code int8_hnsw},
     * {@code bbq_disk}) for this query so it can be surfaced in the profile output. Supplied by the field
     * mapper at query-build time, since only it knows the configured index options. No-op by default.
     */
    default void setQuantization(String quantization) {}

    /**
     * Returns the {@link QueryProfiler} attached to the given searcher, or {@code null} when the searcher is
     * not a {@link ContextIndexSearcher} or profiling is off. kNN queries call this at the head and tail of
     * their {@code rewrite()} to self-enable and self-publish their {@code knn_profile} breakdown, which is
     * what makes profiling behave identically in the DFS and query phases.
     */
    static QueryProfiler activeProfiler(IndexSearcher searcher) {
        return searcher instanceof ContextIndexSearcher contextIndexSearcher ? contextIndexSearcher.getProfiler() : null;
    }
}
