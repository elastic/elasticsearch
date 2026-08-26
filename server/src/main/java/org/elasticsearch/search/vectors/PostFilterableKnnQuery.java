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
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

/**
 * Interface for KNN queries that support post-filtering with retry.
 * Implemented by the HNSW query classes ({@link ESKnnFloatVectorQuery},
 * {@link ESKnnByteVectorQuery}, and their diversifying-children variants).
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

    record OversampledParams(int scaledK, int scaledNumCands) {}

    /**
     * Sizes round 1 of a post-filter search from the target {@code k}, the configured
     * {@code numCands} and the estimated filter {@code selectivity}.
     * <p>
     * {@code scaledK} follows the binomial-variance model in {@link #POST_FILTER_OVERSAMPLE_Z_SCORE}:
     * enough candidates that, after the filter drops a {@code (1 - selectivity)} fraction, {@code k}
     * still survives with high probability, clamped below by {@link #POST_FILTER_OVERSAMPLE_FLOOR}×
     * and above by {@code NUM_CANDS_LIMIT}. {@code scaledNumCands} keeps the exploration budget at
     * least as wide as {@code scaledK}, otherwise the search cannot surface that many candidates,
     * without exceeding {@code NUM_CANDS_LIMIT}.
     */
    static OversampledParams computeOversampledParams(int k, int numCands, float selectivity) {
        double zMargin = zMargin(k, selectivity);
        int scaledK = (int) Math.clamp(
            Math.ceil((k + zMargin) / selectivity),
            Math.ceil(k * POST_FILTER_OVERSAMPLE_FLOOR),
            NUM_CANDS_LIMIT
        );
        int scaledNumCands = Math.clamp(numCands, scaledK, NUM_CANDS_LIMIT);
        return new OversampledParams(scaledK, scaledNumCands);
    }

    /**
     * Creates a new query for the next retry round.
     * <p>
     * For HNSW: {@code excludedDocs} becomes an {@link ExcludeDocsQuery} filter (which Lucene's
     * {@code AbstractKnnVectorQuery#rewrite} converts into {@code AcceptDocs}), and {@code seedDocsPerLeaf}
     * (filter-passing docs only) feed the {@code SeededRetryCollectorManager} as graph entry points.
     * <p>
     * For IVF: {@code excludedDocs} are composed into {@code AcceptDocs} so the codec skips them
     * during posting-list iteration; {@code seedDocsPerLeaf} are ignored.
     *
     * @param reader           the index reader
     * @param excludedDocs     all docs returned across previous rounds, flat and sorted (skip from results)
     * @param seedDocsPerLeaf  per-leaf seed doc IDs (global doc IDs, sorted ascending within each leaf),
     *                         indexed by leaf ordinal, used as starting points for the knn search
     * @param remainingK       how many top results we aim to return after retrying
     */
    Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[][] seedDocsPerLeaf, int remainingK);

    /**
     * @return true if {@code seedDocsPerLeaf} contains at least one seed doc in any leaf, i.e. there is
     * something to seed the retry search with.
     */
    static boolean hasSeeds(int[][] seedDocsPerLeaf) {
        if (seedDocsPerLeaf == null) {
            return false;
        }
        for (int[] leafSeeds : seedDocsPerLeaf) {
            if (leafSeeds != null && leafSeeds.length > 0) {
                return true;
            }
        }
        return false;
    }

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
     * Per-leaf candidate docs collected during the delegate's {@code mergeLeafResults}, indexed
     * by leaf ordinal. Each non-null entry contains the full candidate pool for that leaf with
     * global doc IDs and scores. The orchestrator walks each leaf to partition candidates into
     * filter-matching and filtered-out sets without per-doc {@code subIndex} lookups.
     * <p>
     * Never returns {@code null}: implementations that have not run yet return an empty per-leaf
     * array (every entry {@code null}), so the orchestrator can iterate without a null check.
     */
    default ScoreDoc[][] getPostFilterCandidates() {
        return new ScoreDoc[0][];
    }

    /**
     * Groups arbitrary-order per-leaf {@link TopDocs} into a {@code ScoreDoc[][]} indexed by
     * leaf ordinal. Lucene's {@code AbstractKnnVectorQuery} passes {@code mergeLeafResults} an
     * array sourced from {@code HashMap.values()} whose iteration order is unspecified, so this
     * method resolves each entry's leaf via {@link ReaderUtil#subIndex}.
     * <p>
     * Each leaf's {@link ScoreDoc} array is cloned so the orchestrator can sort it in place
     * without mutating the delegate's {@link TopDocs}.
     */
    static ScoreDoc[][] buildPerLeafCandidates(TopDocs[] perLeafResults, List<LeafReaderContext> leaves) {
        ScoreDoc[][] perLeafCandidates = new ScoreDoc[leaves.size()][];
        for (TopDocs td : perLeafResults) {
            if (td.scoreDocs.length == 0) continue;
            int leafOrd = ReaderUtil.subIndex(td.scoreDocs[0].doc, leaves);
            perLeafCandidates[leafOrd] = td.scoreDocs.clone();
        }
        return perLeafCandidates;
    }
}
