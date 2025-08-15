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

public class MaxScoreTopKnnCollector extends AbstractMaxScoreKnnCollector {

    private long minCompetitiveDocScore = Long.MIN_VALUE;
    private float minCompetitiveSimilarity = Float.NEGATIVE_INFINITY;
    protected final NeighborQueue queue;

    public MaxScoreTopKnnCollector(int k, long visitLimit, KnnSearchStrategy searchStrategy) {
        super(k, visitLimit, searchStrategy);
        this.minCompetitiveDocScore = Long.MAX_VALUE;
        this.queue = new NeighborQueue(k, false);
    }

    @Override
    public long getMinCompetitiveDocScore() {
        return Math.max(queue.peek(), minCompetitiveDocScore);
    }

    @Override
    void updateMinCompetitiveDocScore(long minCompetitiveDocScore) {
        if (this.minCompetitiveDocScore < minCompetitiveDocScore) {
            this.minCompetitiveDocScore = minCompetitiveDocScore;
            this.minCompetitiveSimilarity = queue.decodeScore(minCompetitiveDocScore);
        }
    }

    @Override
    public boolean collect(int docId, float similarity) {
        if (queue.size() >= k()) {
            if (similarity < minCompetitiveSimilarity) {
                return false;
            }
            return queue.insertWithOverflow(docId, similarity);
        } else {
            return queue.insertWithOverflow(docId, similarity);
        }
    }

    @Override
    public int numCollected() {
        return queue.size();
    }

    @Override
    public float minCompetitiveSimilarity() {
        if (queue.size() >= k()) {
            return Math.max(minCompetitiveSimilarity, queue.decodeScore(queue.peek()));
        }
        return Float.NEGATIVE_INFINITY;
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
