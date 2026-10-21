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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.vectors.KnnQueryUtils.createFilterWeight;
import static org.elasticsearch.search.vectors.KnnQueryUtils.dedupAndSelectTopK;
import static org.elasticsearch.search.vectors.KnnQueryUtils.mergeScoreDocArrays;

/**
 * A query that wraps a {@link PostFilterableKnnQuery} and applies post-filtering: it runs the delegate
 * without the user filter, filters the raw per-leaf candidates itself, and re-enters a leaf only when that
 * leaf may still hold a result the filtered pool is missing. See {@link PostFilterableKnnQuery#createRetryQuery}.
 * <p>
 * Two sizes matter here and must not be conflated. {@link #k} is the <em>final</em> result count the user
 * asked for. {@link PostFilterableKnnQuery#postFilterExpectedBaseQueryDocMatches(List)} is the larger pool that the final scoring pass
 * consumes - an outer {@code RescoreKnnVectorQuery}, or
 * {@link PostFilterableKnnQuery#finalizeTopK} for auto-calibrated IVF.
 *
 * <h2>When is the filtered pool complete?</h2>
 * Each leaf hands back its candidates in score order, so every doc a leaf has <em>not</em> shown yet scores at
 * or below the lowest candidate it did show - that leaf's <em>floor</em>. Once the survivors of all leaves are
 * merged into the pool, the pool's last entry is the <em>threshold</em> any missing doc would have to beat. A
 * leaf whose floor is still above the threshold is <em>exhausted</em>: everything it showed was either filtered
 * out or ranks above the threshold, and its next, unseen doc could be a passing one that belongs in the pool.
 * The pool is proven complete only when no leaf is exhausted. This is the same test Lucene's own
 * {@code AbstractKnnVectorQuery} applies between its optimistic first pass and its re-entry pass, just against
 * the <em>filtered</em> threshold - pre-filtering keeps the two in agreement for free, post-filtering has to check.
 * A pool short of its target size has no threshold at all, so every leaf with candidates counts as exhausted
 * and is retried; that subsumes the simpler "retry when short" rule.
 * <p>
 * Exhausted leaves are re-entered with everything already seen excluded (and nothing else searched), up to
 * {@link #MAX_RETRY_ROUNDS} times. If a leaf is still exhausted after that, or the pool is still short, the
 * query falls back to the pre-filtered delegate rather than return a pool it cannot vouch for: post-filtering
 * is an execution optimisation and must not change the result set.
 */
public class PostFilterKnnQuery extends Query implements QueryProfilerProvider {

    // this is compared against filter coverage which is in [0,1]; filters matching at least 70% of the vectors
    // take the post-filter path by default
    public static final float DEFAULT_POST_FILTERING_THRESHOLD = 0.7f;
    // Early-exit check is skipped for small k, where the expected-hits heuristic is too noisy to
    // be meaningful (e.g. k=3, selectivity=0.7 → expected=2.1, threshold=1.05, a single passer
    // would block recovery rounds for no real reason).
    private static final int EARLY_EXIT_MIN_K = 5;
    // Upper bound on retry seed entry points per leaf
    private static final int MAX_SEEDS_PER_LEAF = 4;
    /**
     * Upper bound on retry rounds before giving up on post-filtering and falling back to the pre-filtered
     * delegate. Each round re-enters only the leaves still exhausted, each at least as deep as round 0, so the
     * cost of one query is bounded by {@code (1 + MAX_RETRY_ROUNDS)} round-0 searches plus the fallback.
     */
    static final int MAX_RETRY_ROUNDS = 3;
    private static final Logger logger = LogManager.getLogger(PostFilterKnnQuery.class);

    private final PostFilterableKnnQuery innerQuery;
    private final Query filter;
    /** The final result count the user asked for - not the candidate pool. See the class javadoc. */
    private final int k;
    private final String field;
    private long totalVectorOps;
    private final BitSetProducer parentsFilter;
    private final float postFilterSelectivityThreshold;

    public PostFilterKnnQuery(
        PostFilterableKnnQuery innerQuery,
        Query filter,
        int k,
        String field,
        BitSetProducer parentsFilter,
        float postFilterSelectivityThreshold
    ) {
        assert filter != null : "filter must not be null for PostFilterKnnQuery";
        this.innerQuery = innerQuery;
        this.filter = filter;
        this.k = k;
        this.field = field;
        this.parentsFilter = parentsFilter;
        this.postFilterSelectivityThreshold = postFilterSelectivityThreshold;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        var filterResult = createFilterWeight(searcher, filter, field);
        if (filterResult == KnnQueryUtils.FilterWeight.MATCH_NO_DOCS) {
            return MatchNoDocsQuery.INSTANCE;
        }
        Weight filterWeight = filterResult == null ? null : filterResult.weight();
        // need to check if this is actually a valid candidate for post filtering
        PostFilterRewriteMeta rewriteMeta = maybeCreatePostFilterQuery(searcher, filterWeight);
        if (rewriteMeta != null) {
            assert rewriteMeta.postFilterQuery() instanceof PostFilterableKnnQuery
                : "[createPostFilterQuery] should have generated a PostFilterableKnnQuery";
            var rewritten = postFilterRewrite(searcher, rewriteMeta, filterWeight);
            if (rewritten != null) {
                return rewritten;
            }
        }
        // We fall back to the bare inner query either when the filter does not meet the
        // necessary selectivity (no post-filter rounds ran at all) or when post-filtering
        // could not prove its pool complete within the retry budget.
        Query rewritten = ((Query) innerQuery).rewrite(searcher);
        this.totalVectorOps += innerQuery.totalVectorOps();
        return rewritten;
    }

    private Query postFilterRewrite(IndexSearcher searcher, PostFilterRewriteMeta postFilterRewriteMeta, Weight filterWeight)
        throws IOException {
        PostFilterableKnnQuery postFilterQuery = (PostFilterableKnnQuery) postFilterRewriteMeta.postFilterQuery;
        Query delegate = (Query) postFilterQuery;
        IndexReader reader = searcher.getIndexReader();
        List<LeafReaderContext> leaves = reader.leaves();

        int poolSize = postFilterRewriteMeta.expectedBaseQueryDocMatches;
        float selectivity = postFilterRewriteMeta.selectivity;

        // round 0: initial post-filter search. delegateK is the scaled K already baked into the
        // delegate by createPostFilterDelegate.
        int delegateK = postFilterQuery.k();
        var topDocs = searcher.search(delegate, delegateK);
        if (topDocs.scoreDocs.length == 0) {
            return null;
        }
        long vectorOps = postFilterQuery.totalVectorOps();

        FilteredCandidates filtered = applyFilter(postFilterQuery.getPostFilterCandidates(), filterWeight, leaves);
        ScoreDoc[] pool = dedupAndSelectTopK(flattenPerLeaf(filtered.matchingPerLeaf()), poolSize);

        if (pool.length == 0 || shouldExitEarly(pool.length, selectivity)) {
            this.totalVectorOps += vectorOps;
            return null;
        }

        // State carried across rounds. The floor of a leaf is only ever replaced when that leaf is re-entered:
        // a leaf left alone keeps the floor it already proved, and one that comes back empty is drained (NaN).
        float[] leafFloor = filtered.minScorePerLeaf();
        // Excluded accumulates every doc seen so far; exclusion must be computed before seed selection, which
        // reorders the matching arrays in place.
        int[] excluded = seenDocIds(filtered, reader);
        int[][] seedsPerLeaf = nearestSeedsPerLeaf(filtered.matchingPerLeaf(), MAX_SEEDS_PER_LEAF);

        boolean[] exhausted = exhaustedLeaves(leafFloor, pool, poolSize);
        int round = 0;
        while (exhausted != null && round < MAX_RETRY_ROUNDS) {
            round++;
            // Ask for at least the shortfall (inflated for attrition) and at least as deep as round 0, so an
            // exhausted leaf whose pool is already full still moves its floor a full round's depth.
            int shortfall = Math.max(1, poolSize - pool.length);
            int retryK = Math.max(PostFilterableKnnQuery.computeScaledK(shortfall, selectivity), delegateK);
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "post-filter retry round [{}] for field=[{}], k=[{}], poolSize=[{}], selectivity=[{}], pool so far=[{}],"
                        + " threshold=[{}], leaf floors=[{}], exhausted leaves=[{}], retryK=[{}]",
                    round,
                    field,
                    k,
                    poolSize,
                    selectivity,
                    pool.length,
                    pool.length >= poolSize ? pool[poolSize - 1].score : Float.NEGATIVE_INFINITY,
                    Arrays.toString(leafFloor),
                    Arrays.toString(exhausted),
                    retryK
                );
            }
            Query retry = postFilterQuery.createRetryQuery(reader, excluded, exhausted, seedsPerLeaf, retryK);
            searcher.search(retry, retryK);
            PostFilterableKnnQuery retryQuery = (PostFilterableKnnQuery) retry;
            vectorOps += retryQuery.totalVectorOps();

            FilteredCandidates retryFiltered = applyFilter(retryQuery.getPostFilterCandidates(), filterWeight, leaves);
            float[] retryFloor = retryFiltered.minScorePerLeaf();
            for (int leafOrd = 0; leafOrd < leafFloor.length; leafOrd++) {
                if (exhausted[leafOrd]) {
                    leafFloor[leafOrd] = retryFloor[leafOrd];
                }
            }
            pool = dedupAndSelectTopK(mergeScoreDocArrays(pool, flattenPerLeaf(retryFiltered.matchingPerLeaf())), poolSize);
            excluded = KnnQueryUtils.sortedMerge(excluded, seenDocIds(retryFiltered, reader));
            mergeSeeds(seedsPerLeaf, nearestSeedsPerLeaf(retryFiltered.matchingPerLeaf(), MAX_SEEDS_PER_LEAF));
            exhausted = exhaustedLeaves(leafFloor, pool, poolSize);
        }

        this.totalVectorOps += vectorOps;
        if (exhausted != null) {
            logger.debug(
                "post filtering still has [{}] exhausted leaves after [{}] retry rounds. Falling back to original query",
                countTrue(exhausted),
                round
            );
            return null;
        }
        if (pool.length < poolSize) {
            // No leaf has anything left, yet the pool is short: the index may hold fewer passing docs than the pool
            // wants, or an approximate engine (IVF) may have left some unvisited. The pre-filtered query is the one
            // whose answer we have promised to match, so let it decide.
            logger.debug(
                "post filtering retrieved only [{}] results, less than the desired [{}] results. Falling back to original query",
                pool.length,
                poolSize
            );
            return null;
        }

        // Final scoring pass. Identity unless the inner query owns its own exact rescore (auto-calibrated
        // IVF), in which case the pool is rescored here - after filtering, so no exact comparison is spent
        // on a doc the filter rejected.
        long opsBeforeFinalize = postFilterQuery.totalVectorOps();
        ScoreDoc[] finalDocs = postFilterQuery.finalizeTopK(searcher, pool, k);
        this.totalVectorOps += postFilterQuery.totalVectorOps() - opsBeforeFinalize;
        return new KnnScoreDocQuery(finalDocs, reader);
    }

    /**
     * The leaves whose floor is still above the pool's threshold, or {@code null} when there are none and the
     * pool is therefore proven complete. See the class javadoc.
     *
     * @param leafFloor the lowest score each leaf returned in the last round that searched it; {@code NaN} for a
     *                  leaf with nothing (left) to return, which can never be exhausted
     * @param pool      the filtered pool so far, sorted by score descending
     * @param poolSize  the pool size the orchestrator is filling; a shorter pool has no threshold
     */
    @Nullable
    static boolean[] exhaustedLeaves(float[] leafFloor, ScoreDoc[] pool, int poolSize) {
        float threshold = pool.length >= poolSize ? pool[poolSize - 1].score : Float.NEGATIVE_INFINITY;
        boolean[] exhausted = null;
        for (int leafOrd = 0; leafOrd < leafFloor.length; leafOrd++) {
            // NaN compares false, so a drained leaf is never exhausted
            if (leafFloor[leafOrd] > threshold) {
                if (exhausted == null) {
                    exhausted = new boolean[leafFloor.length];
                }
                exhausted[leafOrd] = true;
            }
        }
        return exhausted;
    }

    private static int countTrue(boolean[] flags) {
        int count = 0;
        for (boolean flag : flags) {
            if (flag) {
                count++;
            }
        }
        return count;
    }

    /**
     * Every doc a round has seen, sorted, for exclusion from later rounds. For nested (block-join) queries a
     * parent we already matched is done - we keep only one hit per parent - so we exclude its whole block; a
     * parent whose child was merely filtered out stays eligible through its untried children. Non-nested
     * queries exclude the individual matched docs.
     */
    private int[] seenDocIds(FilteredCandidates filtered, IndexReader reader) throws IOException {
        ScoreDoc[][] matching = filtered.matchingPerLeaf();
        int[] alreadyMatched = parentsFilter != null
            ? KnnQueryUtils.expandToParentBlocks(matching, reader, parentsFilter)
            : sortedDocIdsFromPerLeaf(matching);
        return KnnQueryUtils.sortedMerge(flattenPerLeafDocIds(filtered.filteredOutPerLeaf()), alreadyMatched);
    }

    /** Replaces the seeds of every leaf that produced new matches; leaves without new matches keep theirs. */
    private static void mergeSeeds(int[][] seedsPerLeaf, int[][] newSeedsPerLeaf) {
        for (int leafOrd = 0; leafOrd < seedsPerLeaf.length; leafOrd++) {
            if (newSeedsPerLeaf[leafOrd] != null) {
                seedsPerLeaf[leafOrd] = newSeedsPerLeaf[leafOrd];
            }
        }
    }

    /**
     * Decides whether to bypass remaining post-filter rounds because the filter is hostile to the
     * kNN topology of this query. Skipped for {@code k < EARLY_EXIT_MIN_K}, where the expected
     * hit count is too small for the heuristic to be informative.
     * <p>
     * For larger {@code k}, the expected number of post-filter hits under independence is
     * {@code k * selectivity}. We bail when the observed count is below half of that - i.e. the
     * filter is letting through fewer than half of what its global selectivity predicts in the
     * kNN region.
     */
    private boolean shouldExitEarly(int scoreDocsCount, float selectivity) {
        if (k < EARLY_EXIT_MIN_K) {
            return false;
        }
        double expectedHits = k * (double) selectivity;
        double threshold = expectedHits * 0.5;
        boolean shouldExit = scoreDocsCount < threshold;
        if (shouldExit) {
            logger.debug(
                "post-filter early exit (hostile filter): field=[{}], k=[{}], selectivity=[{}], "
                    + "scoreDocs=[{}], expectedHits=[{}], threshold=[{}]",
                field,
                k,
                selectivity,
                scoreDocsCount,
                expectedHits,
                threshold
            );
        }
        return shouldExit;
    }

    /**
     * Per-leaf partition of candidates against the filter.
     *
     * @param matchingPerLeaf          per-leaf matching candidates indexed by leaf ordinal (null entries = no
     *                          matches for that leaf). Within each leaf, docs are sorted by global doc ID.
     * @param filteredOutPerLeaf        per-leaf global doc IDs of candidates that did NOT pass the filter,
     *                          indexed by leaf ordinal (null entries = no filtered-out docs for that leaf).
     *                          Within each leaf, sorted ascending by doc ID.
     * @param minScorePerLeaf   the lowest score among <em>all</em> of a leaf's candidates, matching or not - the
     *                          leaf's floor, see the class javadoc - indexed by leaf ordinal; {@code NaN} for a
     *                          leaf that returned no candidates.
     */
    record FilteredCandidates(ScoreDoc[][] matchingPerLeaf, int[][] filteredOutPerLeaf, float[] minScorePerLeaf) {}

    /**
     * Partitions per-leaf candidates into filter-matching and filtered-out sets in one pass
     * per leaf. Within each leaf, candidates are sorted by doc ID and tested by random access
     * via {@link Lucene#asSequentialAccessBits}: when the filter scorer exposes a
     * {@link org.apache.lucene.search.TwoPhaseIterator}, each candidate costs one approximation
     * advance plus one {@code matches()} call. Consuming such scorers through
     * {@code iterator().advance()} instead would have to locate the <em>next matching</em> doc,
     * evaluating the match predicate over every approximation doc in the gap — pathological for
     * filters whose match set is contiguous or sparse (e.g. doc-values ranges, phrases), where one
     * out-of-set candidate can trigger a scan to the end of the segment. Filters without a
     * two-phase view (postings, prebuilt bitsets) take the plain iterator path, which is already
     * cheap for them; eagerly-materializing scorers with no random access (e.g. a bare
     * {@code PointRangeQuery} with no doc-values pairing) still pay their {@code ScorerSupplier#get}
     * cost, same as before. Candidates are sorted in-place.
     */
    static FilteredCandidates applyFilter(ScoreDoc[][] perLeafCandidates, Weight filterWeight, List<LeafReaderContext> leaves)
        throws IOException {

        ScoreDoc[][] matchingPerLeaf = new ScoreDoc[perLeafCandidates.length][];
        int[][] filteredOutPerLeaf = new int[perLeafCandidates.length][];
        float[] minScorePerLeaf = new float[perLeafCandidates.length];
        Arrays.fill(minScorePerLeaf, Float.NaN);

        for (int leafOrd = 0; leafOrd < perLeafCandidates.length; leafOrd++) {
            ScoreDoc[] cands = perLeafCandidates[leafOrd];
            if (cands == null || cands.length == 0) continue;

            LeafReaderContext ctx = leaves.get(leafOrd);
            Arrays.sort(cands, Comparator.comparingInt(sd -> sd.doc));

            // null supplier (no docs match the filter in this leaf) yields MatchNoBits: all filtered out
            ScorerSupplier ss = filterWeight.scorerSupplier(ctx);
            Bits bits = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), ss, cands.length);

            List<ScoreDoc> leafMatching = new ArrayList<>();
            int[] leafFilteredOut = new int[cands.length];
            int filteredOutCount = 0;
            float minScore = Float.POSITIVE_INFINITY;
            for (ScoreDoc cand : cands) {
                minScore = Math.min(minScore, cand.score);
                if (bits.get(cand.doc - ctx.docBase)) {
                    leafMatching.add(cand);
                } else {
                    leafFilteredOut[filteredOutCount++] = cand.doc;
                }
            }
            minScorePerLeaf[leafOrd] = minScore;

            if (leafMatching.isEmpty() == false) {
                matchingPerLeaf[leafOrd] = leafMatching.toArray(new ScoreDoc[0]);
            }
            if (filteredOutCount > 0) {
                filteredOutPerLeaf[leafOrd] = filteredOutCount == leafFilteredOut.length
                    ? leafFilteredOut
                    : Arrays.copyOf(leafFilteredOut, filteredOutCount);
            }
        }

        return new FilteredCandidates(matchingPerLeaf, filteredOutPerLeaf, minScorePerLeaf);
    }

    private static ScoreDoc[] flattenPerLeaf(ScoreDoc[][] perLeaf) {
        int total = 0;
        for (ScoreDoc[] docs : perLeaf) {
            if (docs != null) total += docs.length;
        }
        ScoreDoc[] out = new ScoreDoc[total];
        int pos = 0;
        for (ScoreDoc[] docs : perLeaf) {
            if (docs != null) {
                System.arraycopy(docs, 0, out, pos, docs.length);
                pos += docs.length;
            }
        }
        return out;
    }

    /**
     * Concatenates per-leaf int arrays in leaf-ordinal order. Because {@link #applyFilter}
     * sorts candidates by doc ID within each leaf and docBases increase across leaves,
     * the output is naturally sorted without an explicit sort step.
     */
    private static int[] flattenPerLeafDocIds(int[][] perLeaf) {
        int total = 0;
        for (int[] ids : perLeaf) {
            if (ids != null) total += ids.length;
        }
        int[] out = new int[total];
        int pos = 0;
        for (int[] ids : perLeaf) {
            if (ids != null) {
                System.arraycopy(ids, 0, out, pos, ids.length);
                pos += ids.length;
            }
        }
        return out;
    }

    /**
     * Collects doc IDs from per-leaf matching arrays in leaf-ordinal order. Because
     * {@link #applyFilter} sorts candidates by doc ID within each leaf and docBases
     * increase across leaves, the output is naturally sorted without an explicit sort step.
     */
    private static int[] sortedDocIdsFromPerLeaf(ScoreDoc[][] perLeaf) {
        int total = 0;
        for (ScoreDoc[] docs : perLeaf) {
            if (docs != null) total += docs.length;
        }
        int[] ids = new int[total];
        int pos = 0;
        for (ScoreDoc[] docs : perLeaf) {
            if (docs != null) {
                for (ScoreDoc sd : docs) {
                    ids[pos++] = sd.doc;
                }
            }
        }
        return ids;
    }

    /**
     * Selects up to {@code maxPerLeaf} seed doc IDs per leaf (graph), keeping the highest-scoring
     * (nearest) matches in each leaf so the retry's HNSW search seeds from entry points close to the
     * query. Selection happens here because {@code perLeaf} still carries round-0 scores. Returns an
     * array indexed by leaf ordinal (same indexing as {@code perLeaf}); each sub-array holds that leaf's
     * seed doc IDs sorted ascending, or is {@code null} when the leaf has no matches. {@link
     * SeededRetryCollectorManager} indexes it directly by {@code ctx.ord} — no re-partitioning needed.
     */
    static int[][] nearestSeedsPerLeaf(ScoreDoc[][] perLeaf, int maxPerLeaf) {
        int[][] seedsPerLeaf = new int[perLeaf.length][];
        for (int leafOrd = 0; leafOrd < perLeaf.length; leafOrd++) {
            ScoreDoc[] docs = perLeaf[leafOrd];
            if (docs == null || docs.length == 0) {
                continue;
            }
            int keep = Math.min(docs.length, maxPerLeaf);
            if (keep < docs.length) {
                // Partition so the top-`keep` by score (descending) occupy [0, keep); avoids a full sort.
                ArrayUtil.select(docs, 0, docs.length, keep, Comparator.comparingDouble((ScoreDoc sd) -> sd.score).reversed());
            }
            int[] ids = new int[keep];
            for (int i = 0; i < keep; i++) {
                ids[i] = docs[i].doc;
            }
            Arrays.sort(ids);
            seedsPerLeaf[leafOrd] = ids;
        }
        return seedsPerLeaf;
    }

    private record PostFilterRewriteMeta(Query postFilterQuery, float selectivity, int expectedBaseQueryDocMatches) {}

    private PostFilterRewriteMeta maybeCreatePostFilterQuery(IndexSearcher searcher, Weight filterWeight) throws IOException {
        if (filterWeight == null) {
            return null;
        }
        var leaves = searcher.getIndexReader().leaves();
        float selectivity = innerQuery.estimateFilterSelectivity(filterWeight, leaves);
        if (selectivity >= postFilterSelectivityThreshold) {
            int expectedBaseQueryDocMatches = Math.max(k, innerQuery.postFilterExpectedBaseQueryDocMatches(leaves));
            Query delegate = innerQuery.createPostFilterDelegate(selectivity);
            return new PostFilterRewriteMeta(delegate, selectivity, expectedBaseQueryDocMatches);
        }
        return null;
    }

    /** The final result count this wrapper targets. Package-private: used to pin the mapper's wiring. */
    int k() {
        return k;
    }

    Query innerQuery() {
        assert innerQuery instanceof Query : "[innerQuery] should always be a Query instance";
        return (Query) innerQuery;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(totalVectorOps);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        throw new UnsupportedOperationException("PostFilterKnnQuery does not support [createWeight]. Missing a rewrite?");
    }

    @Override
    public String toString(String field) {
        return "PostFilterKnnQuery[k=" + k + ", innerQuery=" + innerQuery + "]";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        ((Query) innerQuery).visit(visitor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostFilterKnnQuery that = (PostFilterKnnQuery) o;
        return k == that.k
            && Float.compare(postFilterSelectivityThreshold, that.postFilterSelectivityThreshold) == 0
            && innerQuery.equals(that.innerQuery)
            && Objects.equals(filter, that.filter)
            && Objects.equals(parentsFilter, that.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), innerQuery, k, filter, parentsFilter, postFilterSelectivityThreshold);
    }

}
