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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

/**
 * Interface for KNN queries that support post-filtering with retry.
 * Implementations differ in what {@link #k()} means, so the orchestrator never infers the candidate
 * pool from it: see {@link #postFilterExpectedBaseQueryDocMatches(List)} for the pool the orchestrator must fill and
 * {@link #finalizeTopK} for who owns the final (exact) scoring pass.
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
     * The exploration budget to pair with a grown {@code scaledK}, when {@code numCands} is a beam width.
     * <p>
     * HNSW's {@code numCands} <em>is</em> the efSearch beam, so the user's value is kept as-is and only
     * floored at {@code scaledK} - a beam narrower than the number of results asked for cannot surface them -
     * and capped at {@code NUM_CANDS_LIMIT}.
     */
    static int cappedNumCands(int numCands, int scaledK) {
        return Math.clamp(numCands, scaledK, NUM_CANDS_LIMIT);
    }

    /**
     * The binomial-variance round-1 target from {@link #POST_FILTER_OVERSAMPLE_Z_SCORE}: enough candidates
     * that {@code k} still survives a {@code (1 - selectivity)} drop with high probability, floored at
     * {@link #POST_FILTER_OVERSAMPLE_FLOOR}x {@code k} and capped at {@code NUM_CANDS_LIMIT}.
     * <p>
     * The floor is itself capped before clamping: {@code Math.clamp} throws when {@code min > max}, and
     * {@code ceil(k * 1.2)} exceeds {@code NUM_CANDS_LIMIT} for any {@code k > 8333} — a legal request.
     */
    static int computeScaledK(int k, float selectivity) {
        double zMargin = zMargin(k, selectivity);
        double floor = Math.min(Math.ceil(k * POST_FILTER_OVERSAMPLE_FLOOR), NUM_CANDS_LIMIT);
        return (int) Math.clamp(Math.ceil((k + zMargin) / selectivity), floor, NUM_CANDS_LIMIT);
    }

    /**
     * Given a query's own {@code numCands} and {@code k}, returns the {@code numCands} a respawn of it built
     * with {@code newK} should carry, so that the {@code numCands/k} ratio is unchanged. Floored at
     * {@code newK} and capped at {@code NUM_CANDS_LIMIT}.
     * <p>
     * IVF derives its visit ratio (the fraction of a segment it scans) from {@code numCands/k} via
     * {@code IVFVectorsReader#computeDynamicVisitRatio}, so carrying {@code numCands} over unchanged while
     * {@code k} moves silently re-tunes exploration: a larger {@code newK} under-explores, a smaller one (a
     * retry round) over-explores.
     */
    static int numCandsPreservingRatio(int numCands, int k, int newK) {
        if (k <= 0) {
            return Math.clamp(numCands, newK, NUM_CANDS_LIMIT);
        }
        long scaled = (long) Math.ceil((double) numCands * newK / k);
        return Math.clamp(scaled, newK, NUM_CANDS_LIMIT);
    }

    /**
     * Creates a new query for the next retry round. Always called on a delegate returned by
     * {@link #createPostFilterDelegate}, never on the user's own query, so the retry inherits how that
     * delegate collects.
     *
     * @param reader           the index reader
     * @param excludedDocs     all docs returned across previous rounds, flat and sorted (skip from results)
     * @param seedDocsPerLeaf  per-leaf seed doc IDs (global doc IDs, sorted ascending within each leaf),
     *                         indexed by leaf ordinal, used as starting points for the knn search
     * @param remainingK       how many candidates this round should collect: the shortfall in <em>surviving</em>
     *                         docs, already inflated for filter attrition by the orchestrator. Use it as the
     *                         retry's {@code k} as-is; do not scale it again.
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
     * Creates a filter-less delegate query for post-filtering: the round-1 search whose raw candidates the
     * orchestrator filters.
     *
     * <p>The contract is deliberately narrow: collect {@link #computeScaledK}{@code (k(), filterSelectivity)}
     * instead of {@link #k()}, so that {@code k()} of them are still standing once the filter has run, and
     * change nothing else. What an implementation does with a {@code k} - a beam width, a rescore oversample,
     * per-segment collector budgets - is its own business and scales off the inflated {@code k} on its own.
     * This layer must not reach into any of it.
     */
    Query createPostFilterDelegate(float filterSelectivity);

    int countTotalVectors(List<LeafReaderContext> leaves) throws IOException;

    /**
     * Estimated fraction of the vectors this query can actually return that pass {@code filterWeight} -
     * the input to both the post-filter gate and the round-1 sizing in {@link #computeScaledK}.
     * <p>
     * Numerator and denominator must describe the same population, which is why this is one method rather
     * than a count paired with a separate ratio: a query that searches only part of a leaf (a slice) has to
     * narrow both halves together, or the ratio silently drifts - narrowing only the denominator pushes the
     * estimate towards 1 and makes the pool too small.
     * <p>
     * Returns {@code 0} when there is no usable estimate (no vectors visible for the field, or a filter that
     * matches nothing); callers treat that as "do not post-filter" rather than as "perfectly selective".
     */
    default float estimateFilterSelectivity(Weight filterWeight, List<LeafReaderContext> leaves) throws IOException {
        return KnnQueryUtils.computeSelectivity(filterWeight, leaves, countTotalVectors(leaves));
    }

    long totalVectorOps();

    int k();

    /**
     * @return the current numCands for this query. For HNSW this is the efSearch beam width; for IVF it is
     * only meaningful relative to {@link #k()}, whose ratio drives the codec's visit ratio (see
     * {@link #numCandsPreservingRatio}). Used by {@link PostFilterKnnQuery} to size delegate and retry rounds.
     */
    int numCands();

    /**
     * Number of filter-passing candidates the orchestrator should aim to retain before the final scoring
     * pass - i.e. what the outer rescore (or {@link #finalizeTopK}) consumes, not what the user asked for.
     * <p>
     * HNSW returns {@link #k()}: the caller already multiplied {@code k} by the oversample before
     * constructing the query, so {@code k()} <em>is</em> the pool. IVF returns a larger value, because an
     * IVF query is constructed with the final {@code k} and expands the pool itself from its per-segment
     * oversample - which is why {@code leaves} is a parameter. Under auto-calibration that oversample is
     * persisted per segment, so it can only be had by resolving the segments that will actually be searched;
     * configuration alone does not know it.
     */
    default int postFilterExpectedBaseQueryDocMatches(List<LeafReaderContext> leaves) throws IOException {
        return k();
    }

    /**
     * Reduces the filter-passing candidate pool to the final top-{@code finalK}, applying whatever exact
     * scoring pass this query owns. The default is identity: an outer {@code RescoreKnnVectorQuery} (or
     * nothing at all, for unquantized fields) owns final scoring.
     * <p>
     * IVF overrides this for auto-calibrated fields, where the exact rescore normally happens inside
     * {@code rewrite} and is therefore skipped on post-filter delegates - without this hook those results
     * would be returned with raw quantized scores while the fallback path returned exact ones, mixing two
     * score domains inside one search.
     * <p>
     * Returns {@link ScoreDoc}[] rather than a {@link Query} so the implementation can fold the exact
     * comparisons it performed into its own vector-op count; a {@code Query} built inside
     * {@link PostFilterKnnQuery#rewrite} is never visited by the profiler.
     */
    default ScoreDoc[] finalizeTopK(IndexSearcher searcher, ScoreDoc[] candidatePool, int finalK) throws IOException {
        return candidatePool;
    }

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
