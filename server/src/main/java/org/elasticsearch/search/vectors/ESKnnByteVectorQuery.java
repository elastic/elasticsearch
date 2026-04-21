/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;

public class ESKnnByteVectorQuery extends KnnByteVectorQuery implements QueryProfilerProvider, PostFilterableKnnQuery {
    private final int kParam;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private final FixedBitSet seenDocs;
    private final TopDocs seedResults;

    // Written in mergeLeafResults() during rewrite(); read by capturedResults() in the same rewrite chain
    private TopDocs capturedMergedResults;

    public ESKnnByteVectorQuery(String field, byte[] target, int k, int numCands, Query filter, KnnSearchStrategy strategy) {
        this(field, target, k, numCands, filter, strategy, false);
    }

    public ESKnnByteVectorQuery(
        String field,
        byte[] target,
        int k,
        int numCands,
        Query filter,
        KnnSearchStrategy strategy,
        boolean earlyTermination
    ) {
        this(field, target, k, numCands, filter, strategy, earlyTermination, null, null);
    }

    ESKnnByteVectorQuery(
        String field,
        byte[] target,
        int k,
        int numCands,
        Query filter,
        KnnSearchStrategy strategy,
        boolean earlyTermination,
        FixedBitSet seenDocs,
        TopDocs seedResults
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
            ByteVectorValues bvv = ctx.reader().getByteVectorValues(field);
            return bvv != null ? bvv.size() : 0;
        },
            (scaledK, scaledNumCands, strategy, et) -> new ESKnnByteVectorQuery(
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
        this.capturedMergedResults = topK;
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    // --- PostFilterableKnnQuery ---

    @Override
    public TopDocs capturedResults() {
        return capturedMergedResults;
    }

    @Override
    public PostFilterableKnnQuery createRetryQuery(IndexReader reader) {
        FixedBitSet newSeenDocs = PostFilterHelper.buildRetrySeenDocs(seenDocs, capturedMergedResults, reader);
        return new ESKnnByteVectorQuery(
            field,
            getTargetCopy(),
            kParam,
            k,
            new ExcludeDocsQuery(newSeenDocs, reader),
            searchStrategy,
            earlyTermination,
            newSeenDocs,
            capturedMergedResults
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
