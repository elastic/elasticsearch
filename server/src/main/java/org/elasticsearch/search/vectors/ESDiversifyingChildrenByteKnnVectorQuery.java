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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;

public class ESDiversifyingChildrenByteKnnVectorQuery extends DiversifyingChildrenByteKnnVectorQuery
    implements
        QueryProfilerProvider,
        PostFilterableKnnQuery {

    private final int kParam;
    private final int numCands;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private final BitSetProducer parentsFilter;
    private final boolean shouldPostFilter;
    private final FixedBitSet seenDocs;
    private final TopDocs seedResults;

    private TopDocs capturedMergedResults;

    public ESDiversifyingChildrenByteKnnVectorQuery(
        String field,
        byte[] query,
        Query childFilter,
        int k,
        int numCands,
        BitSetProducer parentsFilter,
        KnnSearchStrategy strategy
    ) {
        this(field, query, childFilter, k, numCands, parentsFilter, strategy, false, true, null, null);
    }

    public ESDiversifyingChildrenByteKnnVectorQuery(
        String field,
        byte[] query,
        Query childFilter,
        int k,
        int numCands,
        BitSetProducer parentsFilter,
        KnnSearchStrategy strategy,
        boolean earlyTermination
    ) {
        this(field, query, childFilter, k, numCands, parentsFilter, strategy, earlyTermination, true, null, null);
    }

    ESDiversifyingChildrenByteKnnVectorQuery(
        String field,
        byte[] query,
        Query childFilter,
        int k,
        int numCands,
        BitSetProducer parentsFilter,
        KnnSearchStrategy strategy,
        boolean earlyTermination,
        boolean shouldPostFilter,
        FixedBitSet seenDocs,
        TopDocs seedResults
    ) {
        super(field, query, childFilter, numCands, parentsFilter, strategy);
        this.kParam = k;
        this.numCands = numCands;
        this.earlyTermination = earlyTermination;
        this.parentsFilter = parentsFilter;
        this.shouldPostFilter = shouldPostFilter;
        this.seenDocs = seenDocs;
        this.seedResults = seedResults;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (shouldPostFilter) {
            Query postFiltered = PostFilterHelper.maybePostFilterRewrite(indexSearcher, filter, field, ctx -> {
                ByteVectorValues bvv = ctx.reader().getByteVectorValues(field);
                return bvv != null ? bvv.size() : 0;
            },
                (scaledNumCands, strategy, et) -> new ESDiversifyingChildrenByteKnnVectorQuery(
                    field,
                    getTargetCopy(),
                    null,
                    scaledNumCands,
                    scaledNumCands,
                    parentsFilter,
                    strategy,
                    et,
                    true,
                    null,
                    null
                ),
                kParam,
                searchStrategy,
                earlyTermination,
                ops -> this.vectorOpsCount = ops,
                parentsFilter
            );
            return postFiltered != null ? postFiltered : super.rewrite(indexSearcher);
        } else {
            return super.rewrite(indexSearcher);
        }
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
        return new ESDiversifyingChildrenByteKnnVectorQuery(
            field,
            getTargetCopy(),
            new ExcludeDocsQuery(newSeenDocs, reader),
            kParam,
            numCands,
            parentsFilter,
            searchStrategy,
            earlyTermination,
            true,
            newSeenDocs,
            capturedMergedResults
        );
    }

    @Override
    public long vectorOpsCount() {
        return vectorOpsCount;
    }

    // --- Accessors ---

    public KnnSearchStrategy getStrategy() {
        return searchStrategy;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return PostFilterHelper.wrapCollectorManager(super.getKnnCollectorManager(k, searcher), seedResults, field, earlyTermination);
    }
}
