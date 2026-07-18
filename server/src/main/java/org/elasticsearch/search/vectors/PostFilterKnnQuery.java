/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.vectors.KnnQueryUtils.applyFilter;
import static org.elasticsearch.search.vectors.KnnQueryUtils.computeSelectivity;
import static org.elasticsearch.search.vectors.KnnQueryUtils.createFilterWeight;
import static org.elasticsearch.search.vectors.KnnQueryUtils.dedupAndSelectTopK;
import static org.elasticsearch.search.vectors.KnnQueryUtils.mergeScoreDocArrays;

/**
 * A query that wraps a {@link PostFilterableKnnQuery} and applies post-filtering with up to two
 * additional rounds when the initial pass yields fewer than k results.
 * <ol>
 *   <li>Post-filter retry - re-runs the post-filter delegate while avoiding previously visited
 *       results (doc IDs for HNSW, centroid posting lists for IVF). Asks for the remainder.
 *       See {@link PostFilterableKnnQuery#createRetryQuery}.</li>
 *   <li>Augmented pre-filter fallback - switches modes from post-filter to pre-filter, applying
 *       the original filter combined with an {@code ExcludeDocsQuery} over the already-collected
 *       docs and asking for the remaining {@code k - scoreDocs.length} only.
 *       See {@link PostFilterableKnnQuery#createFallbackQuery}.</li>
 * </ol>
 * If both extra rounds still leave the merged result short of k, the partial result is returned
 * as-is - the caller is expected to tolerate fewer than k hits when the filter genuinely admits
 * fewer matching docs. Only when no post-filter results were ever produced (round 0 found
 * nothing) does the outer rewrite fall through to the bare inner query.
 */
public class PostFilterKnnQuery extends Query implements QueryProfilerProvider {

    // this is compared against filter coverage which is in [0,1], so this marks it essentially off by default
    public static final float DEFAULT_POST_FILTERING_THRESHOLD = 1f;
    // Early-exit check is skipped for small k, where the expected-hits heuristic is too noisy to
    // be meaningful (e.g. k=3, selectivity=0.7 → expected=2.1, threshold=1.05, a single passer
    // would block recovery rounds for no real reason).
    private static final int EARLY_EXIT_MIN_K = 5;
    private static final Logger logger = LogManager.getLogger(PostFilterKnnQuery.class);

    private final PostFilterableKnnQuery innerQuery;
    private final Query filter;
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
        // When profiling is on, this query is the single authority for its subtree's knn_profile: each
        // post-filter round is profiled in isolation and captured here, rather than letting the rounds
        // clobber the shared breakdown. See PostFilterProfiler.
        QueryProfiler profiler = QueryProfilerProvider.activeProfiler(searcher);
        PostFilterProfiler postFilterProfiler = profiler != null ? new PostFilterProfiler() : null;

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
            if (postFilterProfiler != null) {
                postFilterProfiler.selectivity = rewriteMeta.selectivity();
                postFilterProfiler.threshold = postFilterSelectivityThreshold;
            }
            var rewritten = postFilterRewrite(
                searcher,
                (PostFilterableKnnQuery) rewriteMeta.postFilterQuery(),
                filterWeight,
                rewriteMeta.selectivity(),
                postFilterProfiler
            );
            if (rewritten != null) {
                if (postFilterProfiler != null) {
                    pushPostFilterProfile(profiler, postFilterProfiler);
                }
                return rewritten;
            }
            // Post-filtering engaged but fell short; the bare inner query below produces the final result.
            // Suppress its self-publish so it does not overwrite our post_filter breakdown, and record it as
            // a final fallthrough round instead.
            if (postFilterProfiler != null) {
                PostFilterProfiler.prepare((Query) innerQuery);
            }
        }
        // We fall back to the bare inner query either when the filter does not meet the
        // necessary selectivity (no post-filter rounds ran at all) or when post-filtering
        // produced zero results (so no docs were available to seed the augmented fallback).
        Query rewritten = ((Query) innerQuery).rewrite(searcher);
        this.totalVectorOps += innerQuery.totalVectorOps();
        if (rewriteMeta != null && postFilterProfiler != null) {
            // Engaged path fell through: the bare inner search was suppressed above; capture it as the final
            // round and publish the accumulated post_filter breakdown.
            postFilterProfiler.record("fallthrough", (Query) innerQuery, -1, null, innerQuery.totalVectorOps());
            pushPostFilterProfile(profiler, postFilterProfiler);
        }
        // When post-filtering never engaged (rewriteMeta == null), the bare inner query self-publishes a
        // normal (non post_filter) breakdown during its own rewrite above, so nothing more to do here.
        return rewritten;
    }

    private void pushPostFilterProfile(QueryProfiler profiler, PostFilterProfiler postFilterProfiler) {
        profiler.addVectorOpsCount(totalVectorOps);
        profiler.setKnnProfileBreakdown(postFilterProfiler.toBreakdown(totalVectorOps));
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
            PostFilterProfiler.prepare(delegate);
        }
        TopDocs topDocs = searcher.search(delegate, delegateK);
        long vectorOps = postFilterQuery.totalVectorOps();
        ScoreDoc[] passingDocs = applyFilter(topDocs.scoreDocs, filterWeight, searcher);
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

            int[] seenDocs = trackedDocs((PostFilterableKnnQuery) delegate, topDocs);
            int remaining = k - scoreDocs.length;
            Query retry = postFilterQuery.createRetryQuery(searcher.getIndexReader(), sortedDocIds(scoreDocs), seenDocs, remaining);
            if (postFilterProfiler != null) {
                PostFilterProfiler.prepare(retry);
            }
            TopDocs retryDocs = searcher.search(retry, remaining);
            long retryVectorOps = ((PostFilterableKnnQuery) retry).totalVectorOps();
            Integer retryPassingCount = null;
            if (retryDocs.scoreDocs.length > 0) {
                vectorOps += retryVectorOps;
                ScoreDoc[] retryPassing = applyFilter(retryDocs.scoreDocs, filterWeight, searcher);
                retryPassingCount = retryPassing.length;
                scoreDocs = dedupAndSelectTopK(mergeScoreDocArrays(scoreDocs, retryPassing), searcher.getIndexReader(), parentsFilter, k);
            }
            if (postFilterProfiler != null) {
                postFilterProfiler.record("retry", retry, retryDocs.scoreDocs.length, retryPassingCount, retryVectorOps);
            }
        }

        // Augmented pre-filter fallback. When the post-filter rounds yielded some - but fewer
        // than k - results, switch from post-filter to pre-filter mode: ask the inner query to
        // search again with the original filter combined with an ExcludeDocsQuery over the docs
        // we already collected, requesting only the remainder. The collected scoreDocs are kept
        // and merged with the new ones rather than discarded.
        if (scoreDocs.length < k) {
            logger.debug(
                "post-filter augmented fallback firing for field=[{}], k=[{}], selectivity=[{}], scoreDocs so far=[{}]",
                field,
                k,
                selectivity,
                scoreDocs.length
            );
            int remaining = k - scoreDocs.length;
            // add oversampling if we have a parents bitset
            if (parentsFilter != null) {
                remaining = (int) Math.ceil(1.2f * remaining);
            }
            int[] excludedDocs = sortedDocIds(scoreDocs);
            Query fallback = innerQuery.createFallbackQuery(searcher.getIndexReader(), excludedDocs, remaining);
            if (postFilterProfiler != null) {
                PostFilterProfiler.prepare(fallback);
            }
            TopDocs fallbackDocs = searcher.search(fallback, remaining);
            long fallbackVectorOps = ((PostFilterableKnnQuery) fallback).totalVectorOps();
            vectorOps += fallbackVectorOps;
            if (postFilterProfiler != null) {
                postFilterProfiler.record("fallback", fallback, fallbackDocs.scoreDocs.length, null, fallbackVectorOps);
            }
            // No applyFilter() - the fallback already pre-filtered with the original filter.
            scoreDocs = dedupAndSelectTopK(
                mergeScoreDocArrays(scoreDocs, fallbackDocs.scoreDocs),
                searcher.getIndexReader(),
                parentsFilter,
                k
            );
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
     * fallback, and a bare fallthrough). Each round's inner kNN query is profiled in isolation: we suppress
     * its auto-publish (so it does not clobber the shared breakdown) but enable its own collection via
     * {@link #prepare}, then harvest it in {@link #record}. This keeps {@link PostFilterKnnQuery} the single
     * authority for its subtree's {@code knn_profile} and avoids double-counting vector ops.
     */
    private static final class PostFilterProfiler {
        private final List<Map<String, Object>> rounds = new ArrayList<>();
        private float selectivity;
        private float threshold;
        private boolean earlyExit;

        /** Let a round's inner query collect its own breakdown without publishing it to the shared profiler. */
        static void prepare(Query roundQuery) {
            if (roundQuery instanceof QueryProfilerProvider queryProfilerProvider) {
                queryProfilerProvider.setProfilingSuppressed(true);
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
            if (roundQuery instanceof QueryProfilerProvider queryProfilerProvider) {
                QueryProfiler roundProfiler = new QueryProfiler();
                queryProfilerProvider.profile(roundProfiler);
                Map<String, Object> inner = roundProfiler.getKnnProfileBreakdown();
                if (inner != null) {
                    round.put("inner", inner);
                }
            }
            rounds.add(round);
        }

        Map<String, Object> toBreakdown(long totalVectorOps) {
            Map<String, Object> postFilter = new LinkedHashMap<>();
            postFilter.put("selectivity", selectivity);
            postFilter.put("threshold", threshold);
            postFilter.put("early_exit", earlyExit);
            postFilter.put("rounds", rounds);
            postFilter.put("total_vector_ops", totalVectorOps);
            Map<String, Object> breakdown = new LinkedHashMap<>();
            String algorithm = firstRoundAlgorithm();
            if (algorithm != null) {
                breakdown.put("algorithm", algorithm);
            }
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

    private static int[] trackedDocs(PostFilterableKnnQuery delegate, TopDocs topDocs) {
        int[] roundDocs = delegate.getTrackedDocs();
        if (roundDocs.length == 0) {
            roundDocs = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < roundDocs.length; i++) {
                roundDocs[i] = topDocs.scoreDocs[i].doc;
            }
        }
        Arrays.sort(roundDocs);
        return roundDocs;
    }

    private record PostFilterRewriteMeta(Query postFilterQuery, float selectivity) {}

    private PostFilterRewriteMeta maybeCreatePostFilterQuery(IndexSearcher searcher, Weight filterWeight) throws IOException {
        if (filterWeight == null) {
            return null;
        }
        var leaves = searcher.getIndexReader().leaves();
        int totalVectors = innerQuery.countTotalVectors(leaves);
        float selectivity = computeSelectivity(filterWeight, leaves, totalVectors);
        if (selectivity >= postFilterSelectivityThreshold) {
            return new PostFilterRewriteMeta(innerQuery.createPostFilterDelegate(selectivity), selectivity);
        }
        return null;
    }

    private static int[] sortedDocIds(ScoreDoc[] scoreDocs) {
        int[] ids = new int[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            ids[i] = scoreDocs[i].doc;
        }
        Arrays.sort(ids);
        return ids;
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
