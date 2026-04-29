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
import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

/**
 * A query that wraps a {@link PostFilterableKnnQuery} and applies post-filtering with retry.
 * When post-filtering yields fewer than k results, retries with new innerQueries that avoid
 * re-visiting previously seen results (doc IDs for HNSW, centroid posting lists for IVF).
 * <p>
 * The retry loop runs internally (up to {@link #MAX_ROUNDS} rounds). Each round:
 * 1. Executes the innerQuery's search via rewrite
 * 2. Applies the filter to raw results
 * 3. Accumulates filtered results
 * 4. If not enough results, creates a retry innerQuery and continues
 * <p>
 * Each retry round requests {@code ceil(remaining / selectivity)} candidates so the inner KNN
 * search oversamples to compensate for filter rejection (using the precomputed selectivity from
 * {@link KnnQueryUtils#computeSelectivity}). Retry rounds also pass two distinct doc-id arrays:
 * an exclusion set (all docs returned across prior rounds) and a seed set (only filter-passing
 * docs) — see {@link PostFilterableKnnQuery#createRetryQuery}.
 */
public class PostFilterKnnQuery extends Query implements QueryProfilerProvider {

    public static final float DEFAULT_POST_FILTERING_THRESHOLD = 0.7f;
    private static final Logger logger = LogManager.getLogger(PostFilterKnnQuery.class);

    static final int MAX_ROUNDS = 2;

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
        PostFilterRewriteResult result = maybeCreatePostFilterQuery(searcher, filterWeight);
        if (result != null) {
            assert result.postFilterQuery() instanceof PostFilterableKnnQuery
                : "[createPostFilterQuery] should have generated a PostFilterableKnnQuery";
            var rewritten = postFilterRewrite(
                searcher,
                (PostFilterableKnnQuery) result.postFilterQuery(),
                filterWeight,
                result.selectivity()
            );
            if (rewritten != null) {
                return rewritten;
            }
        }
        // we fallback to the original query either when the filter does not meet the necessary selectivity
        // or when after MAX_ROUNDS we still haven't been able to find any relevant results
        Query rewritten = ((Query) innerQuery).rewrite(searcher);
        this.totalVectorOps += innerQuery.totalVectorOps();
        return rewritten;
    }

    private Query postFilterRewrite(IndexSearcher searcher, PostFilterableKnnQuery postFilterQuery, Weight filterWeight, float selectivity)
        throws IOException {
        ScoreDoc[] scoreDocs = new ScoreDoc[0];
        int[] seenDocs = new int[0];
        long vectorOps = 0;
        Query delegate = (Query) postFilterQuery;
        for (int round = 0; round < MAX_ROUNDS; round++) {
            if (round > 0) {
                logger.debug(
                    "post-filter retry round [{}/{}] firing for field=[{}], k=[{}], selectivity=[{}], scoreDocs so far=[{}]",
                    round + 1,
                    MAX_ROUNDS,
                    field,
                    k,
                    selectivity,
                    scoreDocs.length
                );
            }
            // delegateK is the scaled K already baked into the delegate (round 0 via createPostFilterDelegate;
            // round 1+ via createRetryQuery with selectivity-aware requestK).
            int delegateK = ((PostFilterableKnnQuery) delegate).k();
            // todo: check revisiting segments that might have already been exhausted
            TopDocs topDocs = searcher.search(delegate, delegateK);
            if (topDocs.scoreDocs.length == 0) {
                break;
            }
            vectorOps += ((PostFilterableKnnQuery) delegate).totalVectorOps();
            ScoreDoc[] filtered = applyFilter(topDocs.scoreDocs, filterWeight, searcher);
            scoreDocs = mergeResults(scoreDocs, filtered);

            if (parentsFilter != null) {
                scoreDocs = deduplicateByParent(scoreDocs, searcher.getIndexReader(), parentsFilter);
            }
            if (scoreDocs.length >= k) {
                break;
            }

            // Retry path only: drain the per-leaf trackers into seenDocs.
            if (delegate instanceof DocTrackingKnnQuery) {
                int[] roundDocs = ((DocTrackingKnnQuery<?>) delegate).getTrackedDocs();
                if (roundDocs.length == 0) {
                    roundDocs = new int[topDocs.scoreDocs.length];
                    for (int i = 0; i < roundDocs.length; i++) {
                        roundDocs[i] = topDocs.scoreDocs[i].doc;
                    }
                }
                // roundDocs comes back sorted by score from the NeighborQueue drain — sort by docId for merging.
                int[] roundDocsSorted = roundDocs.clone();
                Arrays.sort(roundDocsSorted);
                seenDocs = mergeSortedDedup(seenDocs, roundDocsSorted);
            }

            // Build the retry query for the next round.
            int remaining = k - scoreDocs.length;
            int priorK = ((PostFilterableKnnQuery) delegate).k();
            int priorNumCands = ((PostFilterableKnnQuery) delegate).numCands();
            // Selectivity-aware K: oversample by 1/selectivity so post-filtering yields ~remaining results.
            int requestK = Math.min(NUM_CANDS_LIMIT, (int) Math.ceil(remaining / selectivity));
            // Preserve the prior numCands/k ratio to keep KNN beam width proportional.
            double numCandsRatio = priorK > 0 ? (double) priorNumCands / priorK : 1.0;
            int requestNumCands = Math.min(NUM_CANDS_LIMIT, Math.max(requestK, (int) Math.ceil(requestK * numCandsRatio)));
            int[] passingDocs = sortedDocIds(scoreDocs);
            delegate = ((PostFilterableKnnQuery) delegate).createRetryQuery(
                searcher.getIndexReader(),
                seenDocs,
                passingDocs,
                requestK,
                requestNumCands
            );
        }
        // if after all rounds we still haven't been able to produce k results, we fallback to the original query
        if (scoreDocs.length < k) {
            logger.debug(
                "falling back to original knn query as post filtering retrieved only [{}] results, less than the desired [{}] results.",
                scoreDocs.length,
                k
            );
            return null;
        }
        this.totalVectorOps += vectorOps;
        if (k < scoreDocs.length) {
            scoreDocs = Arrays.copyOf(scoreDocs, k);
        }
        return new KnnScoreDocQuery(scoreDocs, searcher.getIndexReader());
    }

    private record PostFilterRewriteResult(Query postFilterQuery, float selectivity) {}

    private PostFilterRewriteResult maybeCreatePostFilterQuery(IndexSearcher searcher, Weight filterWeight) throws IOException {
        var leaves = searcher.getIndexReader().leaves();
        int totalVectors = innerQuery.countTotalVectors(leaves);
        if (filterWeight == null) {
            return null;
        }
        float selectivity = computeSelectivity(filterWeight, leaves, totalVectors);
        if (selectivity >= postFilterSelectivityThreshold) {
            return new PostFilterRewriteResult(innerQuery.createPostFilterDelegate(selectivity), selectivity);
        }
        return null;
    }

    /**
     * Linear-merge two sorted int[] arrays with deduplication of equal values.
     * Both inputs must be sorted ascending. Output is sorted ascending and unique.
     */
    static int[] mergeSortedDedup(int[] a, int[] b) {
        if (a.length == 0) return b;
        if (b.length == 0) return a;
        int[] result = new int[a.length + b.length];
        int i = 0, j = 0, w = 0;
        while (i < a.length && j < b.length) {
            int ai = a[i];
            int bj = b[j];
            if (ai < bj) {
                result[w++] = ai;
                i++;
            } else if (ai > bj) {
                result[w++] = bj;
                j++;
            } else {
                result[w++] = ai;
                i++;
                j++;
            }
        }
        while (i < a.length) {
            result[w++] = a[i++];
        }
        while (j < b.length) {
            result[w++] = b[j++];
        }
        return w == result.length ? result : Arrays.copyOf(result, w);
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
