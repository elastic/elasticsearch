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
 * A query that wraps a {@link PostFilterableKnnQuery} and applies post-filtering with retry.
 * When post-filtering yields fewer than k results, retries with new innerQueries that avoid
 * re-visiting previously seen results (doc IDs for HNSW, centroid posting lists for IVF).
 * <p>
 * The retry loop runs internally (up to {@link #MAX_ROUNDS} rounds). Each round:
 * 1. Executes the innerQuery's search via rewrite
 * 2. Applies the filter to raw results
 * 3. Accumulates filtered results
 * 4. If not enough results, creates a retry innerQuery and continues
 */
public class PostFilterKnnQuery extends Query implements QueryProfilerProvider {

    public static final float POST_FILTERING_THRESHOLD = 0.7f;
    private static final Logger logger = LogManager.getLogger(PostFilterKnnQuery.class);

    static final int MAX_ROUNDS = 3;

    private final PostFilterableKnnQuery innerQuery;
    private final Query filter;
    private final int k;
    private final String field;
    private long totalVectorOps;
    private final BitSetProducer parentsFilter;

    public PostFilterKnnQuery(PostFilterableKnnQuery innerQuery, Query filter, int k, String field, BitSetProducer parentsFilter) {
        assert filter != null : "filter must not be null for PostFilterKnnQuery";
        this.innerQuery = innerQuery;
        this.filter = filter;
        this.k = k;
        this.field = field;
        this.parentsFilter = parentsFilter;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        var filterWeight = createFilterWeight(searcher, filter, field);
        // need to check if this is actually a valid candidate for post filtering
        var postFilterQuery = maybeCreatePostFilterQuery(searcher, filterWeight);
        if (postFilterQuery != null) {
            assert postFilterQuery instanceof PostFilterableKnnQuery
                : "[createPostFilterQuery] should have generated a PostFilterableKnnQuery";
            var rewritten = postFilterRewrite(searcher, (PostFilterableKnnQuery) postFilterQuery, filterWeight);
            if (rewritten != null) {
                return rewritten;
            }
        }
        // we fallback to the original query either when the filter does not meet the necessary selectivity
        // or wehn after MAX_ROUNDS we still haven't been able to find any relevant results
        Query rewritten = ((Query) innerQuery).rewrite(searcher);
        // this will override any previous work. maybe we could increment instead?
        this.totalVectorOps = innerQuery.totalVectorOps();
        return rewritten;
    }

    private Query postFilterRewrite(IndexSearcher searcher, PostFilterableKnnQuery postFilterQuery, Weight filterWeight)
        throws IOException {
        ScoreDoc[] scoreDocs = new ScoreDoc[0];
        int[] seenDocs = new int[0];
        long vectorOps = 0;
        Query delegate = postFilterQuery.createRetryQuery(searcher.getIndexReader(), null);
        assert delegate instanceof PostFilterableKnnQuery;
        for (int round = 0; round < MAX_ROUNDS; round++) {
            TopDocs topDocs = searcher.search(delegate, Integer.MAX_VALUE);
            if (topDocs.scoreDocs.length == 0) {
                break;
            }
            vectorOps += ((PostFilterableKnnQuery) delegate).totalVectorOps();

            // accumulate this round's doc IDs into the running sorted array
            int[] roundDocs = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                roundDocs[i] = topDocs.scoreDocs[i].doc;
            }
            int[] merged = new int[seenDocs.length + roundDocs.length];
            System.arraycopy(seenDocs, 0, merged, 0, seenDocs.length);
            System.arraycopy(roundDocs, 0, merged, seenDocs.length, roundDocs.length);
            Arrays.sort(merged);
            seenDocs = merged;

            ScoreDoc[] filtered = applyFilter(topDocs.scoreDocs, filterWeight, searcher);
            scoreDocs = mergeResults(scoreDocs, filtered);

            if (parentsFilter != null) {
                scoreDocs = deduplicateByParent(scoreDocs, searcher.getIndexReader(), parentsFilter);
            }
            if (scoreDocs.length >= k) {
                break;
            }
            delegate = ((PostFilterableKnnQuery) delegate).createRetryQuery(searcher.getIndexReader(), seenDocs);
        }
        // if after all rounds we still haven't been able to produce k results, we fallback to the original query
        if (scoreDocs.length < k) {
            logger.debug(
                "falling back to original knn query as post filtering retrieved only ["
                    + scoreDocs.length
                    + "] results, less than the desired ["
                    + k
                    + "] results."
            );
            return null;
        }
        this.totalVectorOps = vectorOps;
        if (k < scoreDocs.length) {
            scoreDocs = Arrays.copyOf(scoreDocs, k);
        }
        return new KnnScoreDocQuery(scoreDocs, searcher.getIndexReader());
    }

    private Query maybeCreatePostFilterQuery(IndexSearcher searcher, Weight filterWeight) throws IOException {
        var leaves = searcher.getIndexReader().leaves();
        int totalVectors = innerQuery.countTotalVectors(leaves);
        if (filterWeight == null) {
            return null;
        }
        float selectivity = computeSelectivity(filterWeight, leaves, totalVectors);
        if (selectivity >= POST_FILTERING_THRESHOLD) {
            return innerQuery.createPostFilterDelegate(selectivity);
        }
        return null;
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
            && innerQuery.equals(that.innerQuery)
            && Objects.equals(filter, that.filter)
            && Objects.equals(parentsFilter, that.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), innerQuery, k, filter, parentsFilter);
    }

}
