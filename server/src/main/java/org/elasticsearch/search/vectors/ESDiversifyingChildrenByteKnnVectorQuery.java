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
import org.apache.lucene.search.TimeLimitingKnnCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.List;

public class ESDiversifyingChildrenByteKnnVectorQuery extends DiversifyingChildrenByteKnnVectorQuery
    implements
        QueryProfilerProvider,
        PostFilterableKnnQuery {

    private final int kParam;
    private final int numCandsParam;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private final BitSetProducer parentsFilter;
    private final int[][] seedDocsPerLeaf;
    private List<LeafReaderContext> leaves;
    private TopDocs[] rawPerLeafResults;
    private KnnSearchProfileData profileData;
    private String quantization;
    private boolean profilingSuppressed;

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
        int[][] seedDocsPerLeaf
    ) {
        super(field, query, childFilter, numCands, parentsFilter, strategy);
        this.kParam = k;
        this.numCandsParam = numCands;
        this.earlyTermination = earlyTermination;
        this.parentsFilter = parentsFilter;
        this.seedDocsPerLeaf = seedDocsPerLeaf;
    }

    @Override
    public void enableProfiling() {
        profileData = new KnnSearchProfileData();
        profileData.setAlgorithmType("hnsw");
        profileData.setQuantization(quantization);
        profileData.setField(field);
    }

    @Override
    public void setQuantization(String quantization) {
        this.quantization = quantization;
    }

    @Override
    public void setProfilingSuppressed(boolean suppressed) {
        this.profilingSuppressed = suppressed;
    }

    @Override
    protected TopDocs searchLeaf(LeafReaderContext ctx, Weight filterWeight, TimeLimitingKnnCollectorManager cm) throws IOException {
        long start = profileData != null ? System.nanoTime() : 0;
        TopDocs result = super.searchLeaf(ctx, filterWeight, cm);
        if (profileData != null) {
            // totalHits.value() is KnnCollector.visitedCount() — the number of HNSW graph nodes visited
            profileData.addHnswLeafSearch(ctx, field, System.nanoTime() - start, result.totalHits.value(), result.scoreDocs.length);
        }
        return result;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        this.rawPerLeafResults = perLeafResults;
        long start = profileData != null ? System.nanoTime() : 0;
        TopDocs topK = TopDocs.merge(kParam, perLeafResults);
        if (profileData != null) {
            profileData.setMergeTimeNs(System.nanoTime() - start);
            profileData.setEarlyTerminated(topK.totalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        this.leaves = searcher.getIndexReader().leaves();
        // Self-enable when a profiler is attached, so profiling works in both the DFS and query phases without
        // an explicit enableProfiling() call. Suppressed when driven by PostFilterKnnQuery.
        QueryProfiler profiler = QueryProfilerProvider.activeProfiler(searcher);
        if (profiler != null && profilingSuppressed == false && profileData == null) {
            enableProfiling();
        }
        if (profileData != null) {
            profileData.setHnswQueryParams(kParam, getK(), getFilter() != null);
        }
        long start = profileData != null ? System.nanoTime() : 0;
        Query result = super.rewrite(searcher);
        if (profileData != null) {
            profileData.setTotalSearchTimeNs(System.nanoTime() - start);
        }
        if (profiler != null && profilingSuppressed == false) {
            profile(profiler);
        }
        return result;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
        if (profileData != null) {
            queryProfiler.addKnnProfileBreakdown(profileData.toMap());
        }
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[][] seedDocsPerLeaf, int remainingK) {
        Query filter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        return new ESDiversifyingChildrenByteKnnVectorQuery(
            field,
            getTargetCopy(),
            filter,
            remainingK,
            numCandsParam,
            parentsFilter,
            searchStrategy,
            earlyTermination,
            seedDocsPerLeaf
        );
    }

    @Override
    public Query createPostFilterDelegate(float filterSelectivity) {
        var params = PostFilterableKnnQuery.computeOversampledParams(kParam, numCandsParam, filterSelectivity);
        return new ESDiversifyingChildrenByteKnnVectorQuery(
            field,
            getTargetCopy(),
            null,
            params.scaledK(),
            params.scaledNumCands(),
            parentsFilter,
            searchStrategy,
            earlyTermination,
            null
        );
    }

    @Override
    public ScoreDoc[][] getPostFilterCandidates() {
        return rawPerLeafResults == null
            ? new ScoreDoc[leaves.size()][]
            : PostFilterableKnnQuery.buildPerLeafCandidates(rawPerLeafResults, leaves);
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
        if (PostFilterableKnnQuery.hasSeeds(seedDocsPerLeaf)) {
            base = new SeededRetryCollectorManager(base, seedDocsPerLeaf, field);
        }
        return earlyTermination ? PatienceCollectorManager.wrap(base) : base;
    }
}
