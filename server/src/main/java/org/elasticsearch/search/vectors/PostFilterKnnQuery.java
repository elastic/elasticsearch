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
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.search.vectors.KnnQueryUtils.applyFilter;
import static org.elasticsearch.search.vectors.KnnQueryUtils.computeSelectivity;
import static org.elasticsearch.search.vectors.KnnQueryUtils.createFilterWeight;
import static org.elasticsearch.search.vectors.KnnQueryUtils.deduplicateByParent;
import static org.elasticsearch.search.vectors.KnnQueryUtils.mergeResults;

/**
 * A query that wraps a {@link PostFilterableKnnQuery} and applies post-filtering with up to two
 * additional rounds when the initial pass yields fewer than k results.
 * <ol>
 *   <li>Post-filter retry — re-runs the post-filter delegate while avoiding previously visited
 *       results (doc IDs for HNSW, centroid posting lists for IVF). Asks for the remainder.
 *       See {@link PostFilterableKnnQuery#createRetryQuery}.</li>
 *   <li>Augmented pre-filter fallback — switches modes from post-filter to pre-filter, applying
 *       the original filter combined with an {@code ExcludeDocsQuery} over the already-collected
 *       docs and asking for the remaining {@code k - scoreDocs.length} only.
 *       See {@link PostFilterableKnnQuery#createFallbackQuery}.</li>
 * </ol>
 * If both extra rounds still leave the merged result short of k, the partial result is returned
 * as-is — the caller is expected to tolerate fewer than k hits when the filter genuinely admits
 * fewer matching docs. Only when no post-filter results were ever produced (round 0 found
 * nothing) does the outer rewrite fall through to the bare inner query.
 */
public class PostFilterKnnQuery extends Query implements QueryProfilerProvider {

    public static final float DEFAULT_POST_FILTERING_THRESHOLD = 0.7f;
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
        var filterWeight = createFilterWeight(searcher, filter, field);
        // need to check if this is actually a valid candidate for post filtering
        PostFilterRewriteMeta rewriteMeta = maybeCreatePostFilterQuery(searcher, filterWeight);
        if (rewriteMeta != null) {
            assert rewriteMeta.postFilterQuery() instanceof PostFilterableKnnQuery
                : "[createPostFilterQuery] should have generated a PostFilterableKnnQuery";
            var rewritten = postFilterRewrite(
                searcher,
                (PostFilterableKnnQuery) rewriteMeta.postFilterQuery(),
                filterWeight,
                rewriteMeta.selectivity()
            );
            if (rewritten != null) {
                return rewritten;
            }
        }
        // We fall back to the bare inner query either when the filter does not meet the
        // necessary selectivity (no post-filter rounds ran at all) or when post-filtering
        // produced zero results (so no docs were available to seed the augmented fallback).
        Query rewritten = ((Query) innerQuery).rewrite(searcher);
        this.totalVectorOps += innerQuery.totalVectorOps();
        return rewritten;
    }

    private Query postFilterRewrite(IndexSearcher searcher, PostFilterableKnnQuery postFilterQuery, Weight filterWeight, float selectivity)
        throws IOException {
        Query delegate = (Query) postFilterQuery;

        // first pass: initial post-filter search. delegateK is the scaled K already baked into the
        // delegate by createPostFilterDelegate.
        int delegateK = postFilterQuery.k();
        TopDocs topDocs = searcher.search(delegate, delegateK);
        long vectorOps = postFilterQuery.totalVectorOps();
        ScoreDoc[] scoreDocs = applyFilter(topDocs.scoreDocs, filterWeight, searcher);
        if (parentsFilter != null) {
            scoreDocs = deduplicateByParent(scoreDocs, searcher.getIndexReader(), parentsFilter);
        }

        // retry round — single retry if round 0 came up short. Skip when round 0 returned zero docs:
        // we'd have no seeds for the retry and nothing to exclude.
        if (scoreDocs.length < k && topDocs.scoreDocs.length > 0) {
            logger.debug(
                "post-filter retry firing for field=[{}], k=[{}], selectivity=[{}], scoreDocs so far=[{}] and visited=[{}]",
                field,
                k,
                selectivity,
                scoreDocs.length,
                vectorOps
            );

            int[] seenDocs = trackedDocs(delegate, topDocs);
            int remaining = k - scoreDocs.length;
            Query retry = postFilterQuery.createRetryQuery(searcher.getIndexReader(), seenDocs, sortedDocIds(scoreDocs), remaining);
            TopDocs retryDocs = searcher.search(retry, remaining);
            if (retryDocs.scoreDocs.length > 0) {
                vectorOps += ((PostFilterableKnnQuery) retry).totalVectorOps();
                ScoreDoc[] retryFiltered = applyFilter(retryDocs.scoreDocs, filterWeight, searcher);
                scoreDocs = mergeResults(scoreDocs, retryFiltered);
                if (parentsFilter != null) {
                    scoreDocs = deduplicateByParent(scoreDocs, searcher.getIndexReader(), parentsFilter);
                }
            }
        }

        // Augmented pre-filter fallback. When the post-filter rounds yielded some — but fewer
        // than k — results, switch from post-filter to pre-filter mode: ask the inner query to
        // search again with the original filter combined with an ExcludeDocsQuery over the docs
        // we already collected, requesting only the remainder. The collected scoreDocs are kept
        // and merged with the new ones rather than discarded.
        if (scoreDocs.length < k && scoreDocs.length > 0) {
            logger.debug(
                "post-filter augmented fallback firing for field=[{}], k=[{}], selectivity=[{}], scoreDocs so far=[{}]",
                field,
                k,
                selectivity,
                scoreDocs.length
            );
            int remaining = k - scoreDocs.length;
            int[] excludedDocs = sortedDocIds(scoreDocs);
            Query fallback = innerQuery.createFallbackQuery(searcher.getIndexReader(), excludedDocs, remaining);
            TopDocs fallbackDocs = searcher.search(fallback, remaining);
            vectorOps += ((PostFilterableKnnQuery) fallback).totalVectorOps();
            // No applyFilter() — the fallback already pre-filtered with the original filter.
            ScoreDoc[] fallbackScoreDocs = fallbackDocs.scoreDocs;
            if (parentsFilter != null) {
                fallbackScoreDocs = deduplicateByParent(fallbackScoreDocs, searcher.getIndexReader(), parentsFilter);
            }
            scoreDocs = mergeResults(scoreDocs, fallbackScoreDocs);
            if (parentsFilter != null) {
                scoreDocs = deduplicateByParent(scoreDocs, searcher.getIndexReader(), parentsFilter);
            }
        }

        // Accumulate the post-filter attempt's vector ops regardless of outcome so the profile
        // reflects the full cost — the outer rewrite() adds the bare innerQuery's own ops on top
        // only when we return null (zero-result case).
        this.totalVectorOps += vectorOps;
        if (scoreDocs.length == 0) {
            // No post-filter results at all — outer rewrite() falls back to the bare innerQuery.
            return null;
        }
        if (scoreDocs.length < k) {
            logger.warn(
                "post filtering retrieved only [{}] results, less than the desired [{}] results; returning partial result.",
                scoreDocs.length,
                k
            );
        } else if (k < scoreDocs.length) {
            scoreDocs = Arrays.copyOf(scoreDocs, k);
        }
        return new KnnScoreDocQuery(scoreDocs, searcher.getIndexReader());
    }

    /**
     * Returns round-0's collected docs as a sorted docId array (used as both excludedDocs and
     * seedDocs source for the retry round). Prefers the per-leaf trackers exposed by
     * {@link DocTrackingKnnQuery}; otherwise — and for non-tracked delegates such as IVF — falls
     * back to {@code topDocs.scoreDocs} (round-0's top-K). The fallback gives IVF retry a
     * non-empty {@code excludedDocs} so the {@code ExcludeDocsQuery} branch in IVF
     * {@code createRetryQuery} actually fires.
     */
    private static int[] trackedDocs(Query delegate, TopDocs topDocs) {
        int[] roundDocs = delegate instanceof DocTrackingKnnQuery<?> dtkq ? dtkq.getTrackedDocs() : new int[0];
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
        var leaves = searcher.getIndexReader().leaves();
        int totalVectors = innerQuery.countTotalVectors(leaves);
        if (filterWeight == null) {
            return null;
        }
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
        assert innerQuery instanceof Query : "[innerQuery] should have generated a Query";
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
