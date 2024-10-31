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
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TimeLimitingKnnCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;

public class ESKnnFloatVectorQuery extends KnnFloatVectorQuery implements ProfilingQuery {
    private final Integer kParam;
    private long vectorOpsCount;
    private final Float rescoreOversample;

    public ESKnnFloatVectorQuery(String field, float[] target, Integer k, int numCands, Float rescoreOversample, Query filter) {
        super(field, target, adjustCandidates(numCands, rescoreOversample), filter);
        this.kParam = k;
        this.rescoreOversample = rescoreOversample;
    }

    private static int adjustCandidates(int numCands, Float rescoreOversample) {
        return rescoreOversample == null ? numCands : (int) Math.ceil(numCands * rescoreOversample);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        // if k param is set, we get only top k results from each shard
        TopDocs topK = kParam == null ? super.mergeLeafResults(perLeafResults) : TopDocs.merge(kParam, perLeafResults);
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    protected TopDocs approximateSearch(
        LeafReaderContext context,
        Bits acceptDocs,
        int visitedLimit,
        KnnCollectorManager knnCollectorManager
    ) throws IOException {
        TopDocs topDocs = super.approximateSearch(context, acceptDocs, visitedLimit, knnCollectorManager);
        if (rescoreOversample == null) {
            return topDocs;
        }

        BitSet exactSearchAcceptDocs = topDocsToBitSet(topDocs, acceptDocs.length());
        BitSetIterator bitSetIterator = new BitSetIterator(exactSearchAcceptDocs, topDocs.scoreDocs.length);
        QueryTimeout queryTimeout = null;
        if (knnCollectorManager instanceof TimeLimitingKnnCollectorManager timeLimitingKnnCollectorManager) {
            queryTimeout = timeLimitingKnnCollectorManager.getQueryTimeout();
        }
        return exactSearch(context, bitSetIterator, queryTimeout);
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.setVectorOpsCount(vectorOpsCount);
    }

    // Convert TopDocs to BitSet
    private static BitSet topDocsToBitSet(TopDocs topDocs, int numBits) {
        // Create a FixedBitSet with a size equal to the maximum number of documents
        BitSet bitSet = new FixedBitSet(numBits);

        // Iterate through each document in TopDocs
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            // Set the corresponding bit for each doc ID
            bitSet.set(scoreDoc.doc);
        }

        return bitSet;
    }
}
