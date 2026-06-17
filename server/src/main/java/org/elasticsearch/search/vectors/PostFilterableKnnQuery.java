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
    float POST_FILTER_OVERSAMPLE_FLOOR = 1.2f;

    /**
     * Confidence dial for how aggressively round 1 oversizes its candidate request.
     * <p>
     * Round 1 asks the codec for {@code m} candidates and applies the user filter; we want
     * round 1 to return {@code ≥ k} matches on its own so the retry round stays rare. The
     * shard-wide selectivity {@code p} tells us the <em>average</em> pass fraction, but the
     * pass count from any given {@code m} candidates is random. Modeling each candidate as
     * independently passing with probability {@code p} (i.e. {@code Binomial(m, p)}, mean
     * {@code m·p}, std-dev {@code √(m·p·(1-p))}), and solving for the smallest {@code m}
     * that pushes {@code k} a distance of {@code Z} standard deviations below the mean:
     * <pre>
     *   m  ≈  ⌈ ( k     +   Z · √(k · (1 - p) / p) ) / p ⌉
     *           │             │
     *           │             └─ Z·σ safety buffer (this constant scales it)
     *           └─ baseline: enough to hit k on average
     * </pre>
     * So {@code Z} is a confidence knob: round 1 succeeds with probability {@code ≈ Φ(Z)}.
     * Reference points: Z=2 → ~97.7%, Z=2.5 → ~99.4%, Z=3 → ~99.9%. Bigger {@code Z} =
     * more candidates per round-1 call, but fewer retries.
     * <p>
     * Caveat: independence is what we assume in the absence of any signal about how the
     * filter correlates with vector content. Correlated filters can inflate variance beyond
     * binomial — the retry round is the safety net for those cases.
     */
    float POST_FILTER_OVERSAMPLE_Z_SCORE = 2.5f;

    /**
     * The {@code Z · √(k · (1 - p) / p)} safety buffer from the round-1 sizing formula in
     * {@link #POST_FILTER_OVERSAMPLE_Z_SCORE}. Callers add this on top of the {@code k/p}
     * baseline before rounding up to get {@code m}.
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
     * during posting-list iteration; {@code seedDocs} are ignored.
     *
     * @param reader           the index reader
     * @param excludedDocs     all docs returned across previous rounds, sorted (skip from results)
     * @param seedDocs         topdocs from all leaves to be used as starting points for knn search
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

    /**
     * Per-leaf docs collected during first round of post-filtering, used by the retry round.
     */
    default int[] getTrackedDocs() {
        return new int[0];
    }
}
