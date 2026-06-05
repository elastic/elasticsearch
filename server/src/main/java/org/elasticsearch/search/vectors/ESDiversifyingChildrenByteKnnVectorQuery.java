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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

public class ESDiversifyingChildrenByteKnnVectorQuery extends DiversifyingChildrenByteKnnVectorQuery
    implements
        QueryProfilerProvider,
        PostFilterableKnnQuery {

    private final int kParam;
    private final int numCandsParam;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private final BitSetProducer parentsFilter;
    private final int[] seedDocs;

    public ESDiversifyingChildrenByteKnnVectorQuery(
        String field,
        byte[] query,
        Query childFilter,
        int k,
        int numCands,
        BitSetProducer parentsFilter,
        KnnSearchStrategy strategy
    ) {
        this(field, query, childFilter, k, numCands, parentsFilter, strategy, false, null);
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
        this(field, query, childFilter, k, numCands, parentsFilter, strategy, earlyTermination, null);
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
        int[] seedDocs
    ) {
        super(field, query, childFilter, numCands, parentsFilter, strategy);
        this.kParam = k;
        this.numCandsParam = numCands;
        this.earlyTermination = earlyTermination;
        this.parentsFilter = parentsFilter;
        this.seedDocs = seedDocs;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        TopDocs topK = TopDocs.merge(kParam, perLeafResults);
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int remainingK) {
        Query filter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        // Keep the full beam from this query — scaling numCands down with remainingK collapses to a
        // pathologically narrow beam when remainingK is tiny (e.g., 1 of 500), making it likely the
        // retry returns only docs that are already excluded or that fail the post-hoc filter.
        int retryNumCands = Math.clamp(numCandsParam, remainingK, NUM_CANDS_LIMIT);
        return new ESDiversifyingChildrenByteKnnVectorQuery(
            field,
            getTargetCopy(),
            filter,
            remainingK,
            retryNumCands,
            parentsFilter,
            searchStrategy,
            earlyTermination,
            seedDocs
        );
    }

    @Override
    public Query createFallbackQuery(IndexReader reader, int[] excludedDocs, int remainingK) {
        Query newFilter = KnnQueryUtils.augmentFilter(getFilter(), excludedDocs, reader);
        int retryNumCands = Math.clamp(numCandsParam, remainingK, NUM_CANDS_LIMIT);
        return new ESDiversifyingChildrenByteKnnVectorQuery(
            field,
            getTargetCopy(),
            newFilter,
            remainingK,
            retryNumCands,
            parentsFilter,
            searchStrategy,
            earlyTermination,
            null
        );
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        double zMargin = PostFilterableKnnQuery.zMargin(kParam, filterSelectivity);
        int scaledK = (int) Math.clamp(
            Math.ceil((kParam + zMargin) / filterSelectivity),
            Math.ceil(kParam * POST_FILTER_OVERSAMPLE_FLOOR),
            NUM_CANDS_LIMIT
        );
        // Maintain the configured numCands/k ratio so HNSW exploration scales with K.
        int scaledNumCands = (int) Math.min(NUM_CANDS_LIMIT, Math.ceil((double) scaledK * numCandsParam / kParam));
        return new ESDiversifyingChildrenByteKnnVectorQuery(
            field,
            getTargetCopy(),
            null,
            scaledK,
            scaledNumCands,
            parentsFilter,
            searchStrategy,
            earlyTermination,
            null
        ) {
            private List<LeafReaderContext> leaves;
            private ScoreDoc[][] perLeafCandidates;

            @Override
            public Query rewrite(IndexSearcher searcher) throws IOException {
                this.leaves = searcher.getIndexReader().leaves();
                return super.rewrite(searcher);
            }

            @Override
            protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
                perLeafCandidates = PostFilterableKnnQuery.buildPerLeafCandidates(perLeafResults, leaves);
                return super.mergeLeafResults(perLeafResults);
            }

            @Override
            public ScoreDoc[][] getPostFilterCandidates() {
                return perLeafCandidates;
            }
        };
    }

    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        int totalVectors = 0;
        for (LeafReaderContext leaf : leaves) {
            ByteVectorValues fvv = leaf.reader().getByteVectorValues(field);
            if (fvv != null) {
                totalVectors += fvv.size();
            }
        }
        return totalVectors;
    }

    @Override
    public long totalVectorOps() {
        return vectorOpsCount;
    }

    @Override
    public int k() {
        return kParam;
    }

    @Override
    public int numCands() {
        return numCandsParam;
    }

    public KnnSearchStrategy getStrategy() {
        return searchStrategy;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        KnnCollectorManager base = super.getKnnCollectorManager(k, searcher);
        if (seedDocs != null && seedDocs.length > 0) {
            base = new SeededRetryCollectorManager(base, seedDocs, field);
        }
        return earlyTermination ? PatienceCollectorManager.wrap(base) : base;
    }
}
