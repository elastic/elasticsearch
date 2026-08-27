/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.vectors.KnnQueryUtils.computeSelectivity;
import static org.elasticsearch.search.vectors.KnnQueryUtils.createFilterWeight;
import static org.elasticsearch.search.vectors.KnnQueryUtils.dedupAndSelectTopK;
import static org.elasticsearch.search.vectors.KnnQueryUtils.mergeScoreDocArrays;

/**
 * A query that wraps a {@link PostFilterableKnnQuery} and applies post-filtering with a single
 * retry round when the initial pass yields fewer than k results. The retry re-runs the
 * post-filter delegate while avoiding previously visited results (doc IDs for HNSW, centroid
 * posting lists for IVF) and asks for the remainder.
 * See {@link PostFilterableKnnQuery#createRetryQuery}.
 * <p>
 * If the retry still leaves the result short of k, the outer rewrite falls through to the
 * bare inner query (standard pre-filtered search with the full k beam).
 */
public class PostFilterKnnQuery extends Query implements QueryProfilerProvider {

    // this is compared against filter coverage which is in [0,1], so this marks it essentially off by default
    public static final float DEFAULT_POST_FILTERING_THRESHOLD = 1f;
    // Early-exit check is skipped for small k, where the expected-hits heuristic is too noisy to
    // be meaningful (e.g. k=3, selectivity=0.7 → expected=2.1, threshold=1.05, a single passer
    // would block recovery rounds for no real reason).
    private static final int EARLY_EXIT_MIN_K = 5;
    // Upper bound on retry seed entry points per graph (leaf). HNSW converges fastest from entry points
    // near the query, and each extra seed adds per-entry-point traversal overhead with diminishing recall
    // benefit, so we keep at most this many of the nearest (highest-scoring) round-0 matches per leaf.
    private static final int MAX_SEEDS_PER_GRAPH = 4;
    private static final Logger logger = LogManager.getLogger(PostFilterKnnQuery.class);

    private final PostFilterableKnnQuery innerQuery;
    private final Query filter;
    private final int k;
    private final String field;
    private long totalVectorOps;
    private final BitSetProducer parentsFilter;
    private final float postFilterSelectivityThreshold;
    private String quantization;
    private boolean profilingEnabled;
    private boolean profilePublished;
    private Map<String, Object> knnProfileBreakdown;

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
        // When profiling is on, this query is the single authority for its subtree's knn_profile: each
        // post-filter round is profiled in isolation and captured here, rather than letting the rounds
        // clobber the shared breakdown. See PostFilterProfiler.
        QueryProfiler profiler = QueryProfilerProvider.activeProfiler(searcher);
        PostFilterProfiler postFilterProfiler = profiler != null || profilingEnabled
            ? new PostFilterProfiler(postFilterSelectivityThreshold)
            : null;

        var filterResult = createFilterWeight(searcher, filter, field);
        if (filterResult == KnnQueryUtils.FilterWeight.MATCH_NO_DOCS) {
            if (postFilterProfiler != null) {
                postFilterProfiler.matchNoDocs = true;
                publishBreakdown(profiler, postFilterProfiler.toBreakdown(0, field));
            }
            return MatchNoDocsQuery.INSTANCE;
        }
        Weight filterWeight = filterResult == null ? null : filterResult.weight();
        // need to check if this is actually a valid candidate for post filtering
        PostFilterRewriteMeta rewriteMeta = maybeCreatePostFilterQuery(searcher, filterWeight);
        boolean engaged = rewriteMeta.postFilterQuery() != null;
        if (postFilterProfiler != null) {
            postFilterProfiler.engaged = engaged;
            postFilterProfiler.selectivity = rewriteMeta.selectivity();
        }
        if (engaged) {
            assert rewriteMeta.postFilterQuery() instanceof PostFilterableKnnQuery
                : "[createPostFilterQuery] should have generated a PostFilterableKnnQuery";
            var rewritten = postFilterRewrite(
                searcher,
                (PostFilterableKnnQuery) rewriteMeta.postFilterQuery(),
                filterWeight,
                rewriteMeta.selectivity(),
                postFilterProfiler
            );
            if (rewritten != null) {
                if (postFilterProfiler != null) {
                    publishBreakdown(profiler, postFilterProfiler.toBreakdown(totalVectorOps, field));
                }
                return rewritten;
            }
        }
        // We fall back to the bare inner query either when the filter does not meet the
        // necessary selectivity (no post-filter rounds ran at all) or when post-filtering
        // produced zero results (so no docs were available to seed the augmented fallback).
        // Either way the bare inner query produces the final result; enable it ourselves so it does not publish
        // alongside our breakdown, and fold what it collected into ours instead.
        if (postFilterProfiler != null) {
            PostFilterProfiler.prepare((Query) innerQuery);
        }
        Query rewritten = ((Query) innerQuery).rewrite(searcher);
        this.totalVectorOps += innerQuery.totalVectorOps();
        if (postFilterProfiler != null) {
            if (engaged) {
                // Engaged but fell short: capture the bare inner search as a final round.
                postFilterProfiler.record("fallthrough", (Query) innerQuery, -1, null, innerQuery.totalVectorOps());
                publishBreakdown(profiler, postFilterProfiler.toBreakdown(totalVectorOps, field));
            } else {
                publishBreakdown(profiler, postFilterProfiler.notEngagedBreakdown((Query) innerQuery, field));
            }
        }
        return rewritten;
    }

    /**
     * Records the breakdown for this subtree and publishes it, unless a caller enabled profiling on us and so
     * owns the result - in which case the breakdown is only stored, for that caller to harvest through
     * {@link #profile}. Publishing is idempotent: {@link Query#rewrite} carries no once-only contract, so a
     * second invocation would otherwise append a duplicate breakdown and double-count vector ops.
     */
    private void publishBreakdown(QueryProfiler profiler, Map<String, Object> breakdown) {
        this.knnProfileBreakdown = breakdown;
        if (profiler != null && profilingEnabled == false && profilePublished == false) {
            profilePublished = true;
            profile(profiler);
        }
    }

    private Query postFilterRewrite(
        IndexSearcher searcher,
        PostFilterableKnnQuery postFilterQuery,
        Weight filterWeight,
        float selectivity,
        PostFilterProfiler postFilterProfiler
    ) throws IOException {
        Query delegate = (Query) postFilterQuery;

        // first pass: initial post-filter search. delegateK is the scaled K already baked into the
        // delegate by createPostFilterDelegate.
        int delegateK = postFilterQuery.k();
        if (postFilterProfiler != null) {
            applyQuantization(delegate);
            PostFilterProfiler.prepare(delegate);
        }
        var topDocs = searcher.search(delegate, delegateK);
        long vectorOps = postFilterQuery.totalVectorOps();
        if (topDocs.scoreDocs.length == 0) {
            if (postFilterProfiler != null) {
                postFilterProfiler.record("initial", delegate, 0, 0, vectorOps);
            }
            this.totalVectorOps += vectorOps;
            return null;
        }

        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        ScoreDoc[][] perLeafCandidates = postFilterQuery.getPostFilterCandidates();
        FilteredCandidates filtered = applyFilter(perLeafCandidates, filterWeight, leaves);

        ScoreDoc[][] matching = filtered.matchingPerLeaf();
        int[][] filteredOut = filtered.filteredOutPerLeaf();
        ScoreDoc[] passingDocs = flattenPerLeaf(matching);
        ScoreDoc[] scoreDocs = dedupAndSelectTopK(passingDocs, searcher.getIndexReader(), parentsFilter, k);

        if (postFilterProfiler != null) {
            postFilterProfiler.record("initial", delegate, topDocs.scoreDocs.length, passingDocs.length, vectorOps);
        }

        // Exit early when the filter is negatively correlated to the knn query and further rounds are unlikely
        // to recover. Zero passers always exits.
        boolean earlyExit = scoreDocs.length > 0 && shouldExitEarly(scoreDocs.length, selectivity);
        if (postFilterProfiler != null) {
            postFilterProfiler.earlyExit = earlyExit;
        }
        if (scoreDocs.length == 0 || earlyExit) {
            this.totalVectorOps += vectorOps;
            return null;
        }

        // retry round - single retry if round 0 came up short.
        if (scoreDocs.length < k) {
            logger.debug(
                "post-filter retry firing for field=[{}], k=[{}], selectivity=[{}], scoreDocs so far=[{}] and visited=[{}]",
                field,
                k,
                selectivity,
                scoreDocs.length,
                vectorOps
            );

            // Exclude every round-0 candidate so the retry can only surface genuinely new docs: the
            // filtered-out set plus all filter-matching docs (not just the top-k kept after parent
            // dedup, since a collapsed sibling would otherwise be re-collected and waste a slot).
            int[] matchingIds = sortedDocIdsFromPerLeaf(matching);
            int[] excluded = KnnQueryUtils.sortedMerge(flattenPerLeafDocIds(filteredOut), matchingIds);
            // Seeds are the nearest (highest-scoring) round-0 matches per leaf, selected here while the
            // scores are still available on `matching`; excluded still needs the full matching set above.
            int[][] seedDocsPerLeaf = nearestSeedsPerLeaf(matching, MAX_SEEDS_PER_GRAPH);
            int remaining = k - scoreDocs.length;
            Query retry = postFilterQuery.createRetryQuery(searcher.getIndexReader(), excluded, seedDocsPerLeaf, remaining);
            if (postFilterProfiler != null) {
                applyQuantization(retry);
                PostFilterProfiler.prepare(retry);
            }
            TopDocs retryDocs = searcher.search(retry, remaining);
            long retryVectorOps = ((PostFilterableKnnQuery) retry).totalVectorOps();
            Integer retryPassingCount = null;
            if (retryDocs.scoreDocs.length > 0) {
                vectorOps += retryVectorOps;
                ScoreDoc[][] retryCandidates = ((PostFilterableKnnQuery) retry).getPostFilterCandidates();
                ScoreDoc[] retryPassing = flattenPerLeaf(applyFilter(retryCandidates, filterWeight, leaves).matchingPerLeaf());
                retryPassingCount = retryPassing.length;
                scoreDocs = dedupAndSelectTopK(mergeScoreDocArrays(scoreDocs, retryPassing), searcher.getIndexReader(), parentsFilter, k);
            }
            if (postFilterProfiler != null) {
                postFilterProfiler.record("retry", retry, retryDocs.scoreDocs.length, retryPassingCount, retryVectorOps);
            }
        }

        // Accumulate the post-filter attempt's vector ops regardless of outcome so the profile
        // reflects the full cost - the outer rewrite() adds the bare innerQuery's own ops on top
        // only when we return null (zero-result case).
        this.totalVectorOps += vectorOps;
        if (scoreDocs.length < k) {
            logger.debug(
                "post filtering retrieved only [{}] results, less than the desired [{}] results. Falling back to original query",
                scoreDocs.length,
                k
            );
            return null;
        }
        return new KnnScoreDocQuery(scoreDocs, searcher.getIndexReader());
    }

    /**
     * Collects a structured {@code post_filter} breakdown across the post-filter rounds (initial, retry,
     * fallback, and a bare fallthrough). Each round's inner kNN query is profiled in isolation: {@link #prepare}
     * enables its collection, which by that same act withholds its own publication, and {@link #record} then
     * harvests it. This keeps {@link PostFilterKnnQuery} the single authority for its subtree's
     * {@code knn_profile} and avoids double-counting vector ops.
     */
    private static final class PostFilterProfiler {
        private final List<Map<String, Object>> rounds = new ArrayList<>();
        private final float threshold;
        private float selectivity = Float.NaN;
        private boolean engaged;
        private boolean earlyExit;
        private boolean matchNoDocs;

        PostFilterProfiler(float threshold) {
            this.threshold = threshold;
        }

        /**
         * Let a round's inner query collect its own breakdown without publishing it to the shared profiler.
         * Enabling it explicitly is what withholds publication: the round's breakdown belongs nested under
         * {@code post_filter.rounds[]}, harvested by {@link #record}, not appended as a second top-level
         * {@code knn_profile} entry.
         */
        static void prepare(Query roundQuery) {
            if (roundQuery instanceof QueryProfilerProvider queryProfilerProvider) {
                queryProfilerProvider.enableProfiling();
            }
        }

        void record(String name, Query roundQuery, int docsFound, Integer docsPassingFilter, long vectorOps) {
            Map<String, Object> round = new LinkedHashMap<>();
            round.put("name", name);
            if (docsFound >= 0) {
                round.put("docs_found", docsFound);
            }
            if (docsPassingFilter != null) {
                round.put("docs_passing_filter", docsPassingFilter);
            }
            round.put("vector_ops", vectorOps);
            Map<String, Object> inner = harvest(roundQuery);
            if (inner.isEmpty() == false) {
                round.put("inner", inner);
            }
            rounds.add(round);
        }

        /** The breakdown a {@link #prepare}d query collected, or an empty map when it collected none. */
        private static Map<String, Object> harvest(Query roundQuery) {
            if (roundQuery instanceof QueryProfilerProvider queryProfilerProvider) {
                QueryProfiler roundProfiler = new QueryProfiler();
                queryProfilerProvider.profile(roundProfiler);
                // The round query publishes at most one breakdown onto this throwaway profiler.
                List<Map<String, Object>> innerBreakdowns = roundProfiler.getKnnProfileBreakdowns();
                if (innerBreakdowns.isEmpty() == false) {
                    return innerBreakdowns.get(0);
                }
            }
            return Map.of();
        }

        Map<String, Object> toBreakdown(long totalVectorOps, String field) {
            Map<String, Object> postFilter = new LinkedHashMap<>();
            postFilter.put("engaged", engaged);
            if (Float.isNaN(selectivity) == false) {
                postFilter.put("selectivity", selectivity);
            }
            postFilter.put("threshold", threshold);
            postFilter.put("early_exit", earlyExit);
            if (matchNoDocs) {
                postFilter.put("match_no_docs", true);
            }
            postFilter.put("rounds", rounds);
            postFilter.put("total_vector_ops", totalVectorOps);
            Map<String, Object> breakdown = new LinkedHashMap<>();
            String algorithm = firstRoundAlgorithm();
            if (algorithm != null) {
                breakdown.put("algorithm", algorithm);
            }
            if (field != null) {
                breakdown.put("field", field);
            }
            breakdown.put("post_filter", postFilter);
            return breakdown;
        }

        /**
         * Breakdown for the case where the filter was not selective enough for post-filtering to run. The
         * inner query's own breakdown is surfaced at the top level - it is what actually executed, so the
         * shape stays the same as an unwrapped kNN query - annotated with a compact {@code post_filter}
         * section recording that post-filtering was skipped and the numbers behind that decision.
         */
        Map<String, Object> notEngagedBreakdown(Query innerQuery, String field) {
            Map<String, Object> breakdown = new LinkedHashMap<>(harvest(innerQuery));
            breakdown.putIfAbsent("field", field);
            Map<String, Object> postFilter = new LinkedHashMap<>();
            postFilter.put("engaged", false);
            if (Float.isNaN(selectivity) == false) {
                postFilter.put("selectivity", selectivity);
            }
            postFilter.put("threshold", threshold);
            breakdown.put("post_filter", postFilter);
            return breakdown;
        }

        private String firstRoundAlgorithm() {
            for (Map<String, Object> round : rounds) {
                if (round.get("inner") instanceof Map<?, ?> inner && inner.get("algorithm") != null) {
                    return inner.get("algorithm").toString();
                }
            }
            return null;
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
        double threshold = expectedHits / 2.0;
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
     */
    record FilteredCandidates(ScoreDoc[][] matchingPerLeaf, int[][] filteredOutPerLeaf) {}

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
            for (ScoreDoc cand : cands) {
                if (bits.get(cand.doc - ctx.docBase)) {
                    leafMatching.add(cand);
                } else {
                    leafFilteredOut[filteredOutCount++] = cand.doc;
                }
            }

            if (leafMatching.isEmpty() == false) {
                matchingPerLeaf[leafOrd] = leafMatching.toArray(new ScoreDoc[0]);
            }
            if (filteredOutCount > 0) {
                filteredOutPerLeaf[leafOrd] = filteredOutCount == leafFilteredOut.length
                    ? leafFilteredOut
                    : Arrays.copyOf(leafFilteredOut, filteredOutCount);
            }
        }

        return new FilteredCandidates(matchingPerLeaf, filteredOutPerLeaf);
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

    /**
     * @param postFilterQuery the oversampled delegate to drive the post-filter rounds with, or {@code null}
     *                        when post-filtering should not be attempted
     * @param selectivity     the measured fraction of vectors passing the filter, or {@link Float#NaN} when
     *                        there is no filter to measure. Reported even when post-filtering is declined, so
     *                        the profile can explain the decision.
     */
    private record PostFilterRewriteMeta(Query postFilterQuery, float selectivity) {}

    private PostFilterRewriteMeta maybeCreatePostFilterQuery(IndexSearcher searcher, Weight filterWeight) throws IOException {
        if (filterWeight == null) {
            return new PostFilterRewriteMeta(null, Float.NaN);
        }
        var leaves = searcher.getIndexReader().leaves();
        int totalVectors = innerQuery.countTotalVectors(leaves);
        float selectivity = computeSelectivity(filterWeight, leaves, totalVectors);
        if (selectivity >= postFilterSelectivityThreshold) {
            return new PostFilterRewriteMeta(innerQuery.createPostFilterDelegate(selectivity), selectivity);
        }
        return new PostFilterRewriteMeta(null, selectivity);
    }

    Query innerQuery() {
        assert innerQuery instanceof Query : "[innerQuery] should always be a Query instance";
        return (Query) innerQuery;
    }

    @Override
    public void enableProfiling() {
        // Collection is normally self-enabled from the profiler on the searcher; this covers callers that
        // drive the query through a plain IndexSearcher and harvest the breakdown themselves, which is why it
        // also withholds publication. The per-round inner queries are enabled individually in
        // postFilterRewrite, so there is nothing to forward here.
        profilingEnabled = true;
    }

    @Override
    public void setQuantization(String quantization) {
        // The mapper sets quantization on this outer query after the post-filter wrap, so forward it to the
        // wrapped inner query. Per-round delegates are fresh copies built in postFilterRewrite and receive it
        // separately via applyQuantization.
        this.quantization = quantization;
        if (innerQuery instanceof QueryProfilerProvider queryProfilerProvider) {
            queryProfilerProvider.setQuantization(quantization);
        }
    }

    /** Propagates the configured quantization onto a freshly-built per-round delegate so its inner breakdown carries the label. */
    private void applyQuantization(Query roundQuery) {
        if (quantization != null && roundQuery instanceof QueryProfilerProvider queryProfilerProvider) {
            queryProfilerProvider.setQuantization(quantization);
        }
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(totalVectorOps);
        if (knnProfileBreakdown != null) {
            queryProfiler.addKnnProfileBreakdown(knnProfileBreakdown);
        }
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
