/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;

class MaxScoreTopKnnCollector extends AbstractMaxScoreKnnCollector {

    private long minCompetitiveDocScore;
    private float minCompetitiveSimilarity;
    protected final NeighborQueue queue;

    MaxScoreTopKnnCollector(int k, long visitLimit, KnnSearchStrategy searchStrategy) {
        super(k, visitLimit, searchStrategy);
        this.minCompetitiveDocScore = LEAST_COMPETITIVE;
        this.minCompetitiveSimilarity = Float.NEGATIVE_INFINITY;
        this.queue = new NeighborQueue(k, false);
    }

    @Override
    public long getMinCompetitiveDocScore() {
        return queue.size() > 0 ? Math.max(minCompetitiveDocScore, queue.peek()) : minCompetitiveDocScore;
    }

    @Override
    void updateMinCompetitiveDocScore(long minCompetitiveDocScore) {
        long queueMinCompetitiveDocScore = queue.size() > 0 ? queue.peek() : LEAST_COMPETITIVE;
        this.minCompetitiveDocScore = Math.max(this.minCompetitiveDocScore, Math.max(queueMinCompetitiveDocScore, minCompetitiveDocScore));
        this.minCompetitiveSimilarity = NeighborQueue.decodeScoreRaw(this.minCompetitiveDocScore);
    }

    @Override
    public boolean collect(int docId, float similarity) {
        return queue.insertWithOverflow(docId, similarity);
    }

    @Override
    public int numCollected() {
        return queue.size();
    }

    @Override
    public float minCompetitiveSimilarity() {
        return queue.size() < k() ? Float.NEGATIVE_INFINITY : Math.max(minCompetitiveSimilarity, queue.topScore());
    }

    @Override
    public TopDocs topDocs() {
        assert queue.size() <= k() : "Tried to collect more results than the maximum number allowed";
        ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
        for (int i = 1; i <= scoreDocs.length; i++) {
            scoreDocs[scoreDocs.length - i] = new ScoreDoc(queue.topNode(), queue.topScore());
            queue.pop();
        }
        TotalHits.Relation relation = earlyTerminated() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO;
        return new TopDocs(new TotalHits(visitedCount(), relation), scoreDocs);
    }
}
