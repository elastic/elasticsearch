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
     * <p>
     * On the normal search path this does not need to be called explicitly: a kNN query auto-enables when
     * it discovers a {@link QueryProfiler} on the {@link org.elasticsearch.search.internal.ContextIndexSearcher}
     * during {@code rewrite()}. This method remains the explicit entry point for direct callers and tests
     * that run against a plain {@code IndexSearcher}, and for {@link PostFilterKnnQuery} which enables its
     * inner per-round queries by hand. Must be called before {@code rewrite()} to take effect.
     */
    default void enableProfiling() {}

    /**
     * Records the vector quantization / index-options type (e.g. {@code bbq_hnsw}, {@code int8_hnsw},
     * {@code bbq_disk}) for this query so it can be surfaced in the profile output. Supplied by the field
     * mapper at query-build time, since only it knows the configured index options. No-op by default.
     */
    default void setQuantization(String quantization) {}

    /**
     * When {@code true}, this query must not auto-publish its breakdown to the profiler attached to the
     * searcher during {@code rewrite()}. Used by {@link PostFilterKnnQuery}, which drives its inner
     * per-round searches (initial / retry / fallback) itself and captures each round's breakdown
     * explicitly, so those inner searches must not append a separate {@code knn_profile} entry. No-op
     * by default.
     */
    default void setProfilingSuppressed(boolean suppressed) {}

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
