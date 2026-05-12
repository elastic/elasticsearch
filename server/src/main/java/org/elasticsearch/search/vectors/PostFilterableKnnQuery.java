/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.List;

/**
 * Interface for KNN queries that support post-filtering with retry.
 * Implemented by both HNSW ({@link ESKnnFloatVectorQuery}, {@link ESKnnByteVectorQuery})
 * and IVF ({@link IVFKnnFloatVectorQuery}) queries.
 */
public interface PostFilterableKnnQuery {

    /**
     * Minimum round-1 oversample factor. Round 1 always asks for at least this many ×
     * the target count, regardless of what the binomial variance formula computes. Active
     * when selectivity is near 1, where the variance term collapses to ≈ 0.
     */
    float POST_FILTER_OVERSAMPLE_FLOOR = 1.3f;

    /**
     * Confidence-level Z-score for the binomial variance term in round-1 sizing.
     * <p>
     * Selectivity {@code p} is computed exactly across the shard. With no information about
     * correlation between the filter and vector content, the maximum-entropy assumption is
     * independence: pass count from {@code m} candidates is {@code X ~ Binomial(m, p)}. The
     * approximation
     * <pre>
     *   m ≈ ⌈ (k + Z · √(k · (1 - p) / p)) / p ⌉
     * </pre>
     * makes {@code P(X ≥ k) ≈ Φ(Z)} (linearised at the boundary {@code m·p ≈ k};
     * exact at the limit, slightly aggressive for small k). Z=2 → ~97.7%, Z=2.5 → ~99.4%,
     * Z=3 → ~99.9%.
     * <p>
     * If the filter does correlate with vector content (which we can't know up front), the
     * effective variance can exceed binomial; the retry mechanism is the safety net for queries
     * where round 1 falls short despite this margin.
     */
    float POST_FILTER_OVERSAMPLE_Z_SCORE = 2.5f;

    /**
     * Variance buffer term {@code Z · √(k · (1 - p) / p)} from the binomial-variance round-1
     * sizing.
     */
    static double zMargin(int k, float selectivity) {
        return POST_FILTER_OVERSAMPLE_Z_SCORE * Math.sqrt(k * (1.0f - selectivity) / selectivity);
    }

    /**
     * Creates a new query for the next retry round.
     * <p>
     * For HNSW: {@code excludedDocs} becomes an {@link ExcludeDocsQuery} filter (which Lucene's
     * {@code AbstractKnnVectorQuery#rewrite} converts into {@code AcceptDocs}), and {@code seedDocs}
     * (filter-passing docs only) feed the {@code SeededRetryCollectorManager} as graph entry points.
     * <p>
     * For IVF: {@code excludedDocs} are composed into {@code AcceptDocs} so the codec skips them
     * during posting-list iteration; {@code seedDocs} are ignored (IVF has no graph seeding).
     *
     * @param reader           the index reader
     * @param excludedDocs     all docs returned across previous rounds, sorted (skip from results)
     * @param seedDocs         filter-passing docs from previous rounds, sorted (HNSW seeding only)
     * @param remainingK       how many top results we aim to return after retrying
     */
    Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int remainingK);

    /**
     * Reconstructs the KNN query for the augmented pre-filter fallback used by
     * {@link PostFilterKnnQuery} when post-filtering yields some — but fewer than {@code k} —
     * results. The returned query keeps the implementor's original filter as a pre-filter,
     * combined with an {@link ExcludeDocsQuery} over {@code excludedDocs} so already-collected
     * docs are not visited again, and asks for {@code remainingK} results.
     * <p>
     * Unlike {@link #createRetryQuery}, no seed docs are passed — the fallback switches modes
     * from post-filter to pre-filter, so HNSW graph seeding does not apply.
     *
     * @param reader        the index reader
     * @param excludedDocs  docs already collected by the post-filter rounds, sorted
     * @param remainingK    how many additional top results we aim to return ({@code k - collected})
     */
    Query createFallbackQuery(IndexReader reader, int[] excludedDocs, int remainingK);

    /**
     * Creates a filter-less delegate query for post-filtering. Subclasses provide
     * the concrete query type with the appropriate vector data.
     */
    Query createPostFilterDelegate(float filterSelectivity);

    int countTotalVectors(List<LeafReaderContext> leaves) throws IOException;

    long totalVectorOps();

    int k();

    /**
     * @return the current numCands (KNN beam / collector candidate budget) for this query.
     * Used by {@link PostFilterKnnQuery} to compute retry-round numCands scaling.
     */
    int numCands();
}
