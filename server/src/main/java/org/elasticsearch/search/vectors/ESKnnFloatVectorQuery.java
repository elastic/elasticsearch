/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;

public class ESKnnFloatVectorQuery extends KnnFloatVectorQuery implements QueryProfilerProvider, PostFilterableKnnQuery {
    private final int kParam;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private final FixedBitSet seenDocs;
    private final ScoreDoc[] seedResults;

    // Internal plumbing: written in mergeLeafResults() during rewrite, read by search() immediately after
    private ScoreDoc[] lastSearchResults;

    public ESKnnFloatVectorQuery(String field, float[] target, int k, int numCands, Query filter, KnnSearchStrategy strategy) {
        this(field, target, k, numCands, filter, strategy, false);
    }

    public ESKnnFloatVectorQuery(
        String field,
        float[] target,
        int k,
        int numCands,
        Query filter,
        KnnSearchStrategy strategy,
        boolean earlyTermination
    ) {
        this(field, target, k, numCands, filter, strategy, earlyTermination, null, null);
    }

    ESKnnFloatVectorQuery(
        String field,
        float[] target,
        int k,
        int numCands,
        Query filter,
        KnnSearchStrategy strategy,
        boolean earlyTermination,
        FixedBitSet seenDocs,
        ScoreDoc[] seedResults
    ) {
        super(field, target, numCands, filter, strategy);
        this.kParam = k;
        this.earlyTermination = earlyTermination;
        this.seenDocs = seenDocs;
        this.seedResults = seedResults;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query postFiltered = PostFilterHelper.maybePostFilterRewrite(indexSearcher, filter, field, ctx -> {
            FloatVectorValues fvv = ctx.reader().getFloatVectorValues(field);
            return fvv != null ? fvv.size() : 0;
        },
            (scaledK, scaledNumCands, strategy, et) -> new ESKnnFloatVectorQuery(
                field,
                getTargetCopy(),
                scaledK,
                scaledNumCands,
                null,
                strategy,
                et
            ),
            kParam,
            k,
            searchStrategy,
            earlyTermination,
            ops -> this.vectorOpsCount = ops,
            null
        );
        return postFiltered != null ? postFiltered : super.rewrite(indexSearcher);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        TopDocs topK = TopDocs.merge(kParam, perLeafResults);
        this.lastSearchResults = topK.scoreDocs;
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    // --- PostFilterableKnnQuery ---

    @Override
    public ScoreDoc[] findCandidates(IndexSearcher searcher) throws IOException {
        super.rewrite(searcher);
        return lastSearchResults != null ? lastSearchResults : new ScoreDoc[0];
    }

    @Override
    public PostFilterableKnnQuery createRetryQuery(IndexReader reader, ScoreDoc[] previousResults) {
        FixedBitSet newSeenDocs = PostFilterHelper.buildRetrySeenDocs(seenDocs, previousResults, reader);
        return new ESKnnFloatVectorQuery(
            field,
            getTargetCopy(),
            kParam,
            k,
            new ExcludeDocsQuery(newSeenDocs, reader),
            searchStrategy,
            earlyTermination,
            newSeenDocs,
            previousResults
        );
    }

    @Override
    public long vectorOpsCount() {
        return vectorOpsCount;
    }

    // --- Accessors ---

    public int kParam() {
        return kParam;
    }

    public KnnSearchStrategy getStrategy() {
        return searchStrategy;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return PostFilterHelper.wrapCollectorManager(super.getKnnCollectorManager(k, searcher), seedResults, field, earlyTermination);
    }
}
