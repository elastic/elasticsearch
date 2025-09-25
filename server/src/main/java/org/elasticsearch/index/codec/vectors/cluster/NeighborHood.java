/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

public record NeighborHood(int[] neighbors, float maxIntraDistance) {

    private static final int M = 8;
    private static final int EF_CONSTRUCTION = 150;

    static final NeighborHood EMPTY = new NeighborHood(new int[0], Float.POSITIVE_INFINITY);

    public static NeighborHood[] computeNeighborhoods(float[][] centers, int clustersPerNeighborhood) throws IOException {
        assert centers.length > clustersPerNeighborhood;
        // experiments shows that below 10k, we better use brute force, otherwise hnsw gives us a nice speed up
        if (centers.length < 10_000) {
            return computeNeighborhoodsBruteForce(centers, clustersPerNeighborhood);
        } else {
            return computeNeighborhoodsGraph(centers, clustersPerNeighborhood);
        }
    }

    public static NeighborHood[] computeNeighborhoodsBruteForce(float[][] centers, int clustersPerNeighborhood) {
        int k = centers.length;
        NeighborQueue[] neighborQueues = new NeighborQueue[k];
        for (int i = 0; i < k; i++) {
            neighborQueues[i] = new NeighborQueue(clustersPerNeighborhood, true);
        }
        final float[] scores = new float[4];
        final int limit = k - 3;
        for (int i = 0; i < k - 1; i++) {
            float[] center = centers[i];
            int j = i + 1;
            for (; j < limit; j += 4) {
                ESVectorUtil.squareDistanceBulk(center, centers[j], centers[j + 1], centers[j + 2], centers[j + 3], scores);
                for (int h = 0; h < 4; h++) {
                    neighborQueues[j + h].insertWithOverflow(i, scores[h]);
                    neighborQueues[i].insertWithOverflow(j + h, scores[h]);
                }
            }
            for (; j < k; j++) {
                float dsq = VectorUtil.squareDistance(center, centers[j]);
                neighborQueues[j].insertWithOverflow(i, dsq);
                neighborQueues[i].insertWithOverflow(j, dsq);
            }
        }

        NeighborHood[] neighborhoods = new NeighborHood[k];
        for (int i = 0; i < k; i++) {
            NeighborQueue queue = neighborQueues[i];
            if (queue.size() == 0) {
                // no neighbors, skip
                neighborhoods[i] = NeighborHood.EMPTY;
                continue;
            }
            // consume the queue into the neighbors array and get the maximum intra-cluster distance
            int[] neighbors = new int[queue.size()];
            float maxIntraDistance = queue.topScore();
            int iter = 0;
            while (queue.size() > 0) {
                neighbors[neighbors.length - ++iter] = queue.pop();
            }
            neighborhoods[i] = new NeighborHood(neighbors, maxIntraDistance);
        }
        return neighborhoods;
    }

    public static NeighborHood[] computeNeighborhoodsGraph(float[][] centers, int clustersPerNeighborhood) throws IOException {
        final UpdateableRandomVectorScorer scorer = new UpdateableRandomVectorScorer() {
            int scoringOrdinal;
            private final float[] distances = new float[4];

            @Override
            public float score(int node) {
                return VectorUtil.normalizeDistanceToUnitInterval(VectorUtil.squareDistance(centers[scoringOrdinal], centers[node]));
            }

            @Override
            public void bulkScore(int[] nodes, float[] scores, int numNodes) {
                int i = 0;
                final int limit = numNodes - 3;
                for (; i < limit; i += 4) {
                    ESVectorUtil.squareDistanceBulk(
                        centers[scoringOrdinal],
                        centers[nodes[i]],
                        centers[nodes[i + 1]],
                        centers[nodes[i + 2]],
                        centers[nodes[i + 3]],
                        distances
                    );
                    for (int j = 0; j < 4; j++) {
                        scores[i + j] = VectorUtil.normalizeDistanceToUnitInterval(distances[j]);
                    }
                }
                for (; i < numNodes; i++) {
                    scores[i] = score(nodes[i]);
                }
            }

            @Override
            public int maxOrd() {
                return centers.length;
            }

            @Override
            public void setScoringOrdinal(int node) {
                scoringOrdinal = node;
            }
        };
        final RandomVectorScorerSupplier supplier = new RandomVectorScorerSupplier() {
            @Override
            public UpdateableRandomVectorScorer scorer() {
                return scorer;
            }

            @Override
            public RandomVectorScorerSupplier copy() {
                return this;
            }
        };
        final OnHeapHnswGraph graph = HnswGraphBuilder.create(supplier, M, EF_CONSTRUCTION, 42L).build(centers.length);
        final NeighborHood[] neighborhoods = new NeighborHood[centers.length];
        // oversample the number of neighbors we collect to improve recall
        final ReusableKnnCollector collector = new ReusableKnnCollector(2 * clustersPerNeighborhood);
        for (int i = 0; i < centers.length; i++) {
            collector.reset(i);
            scorer.setScoringOrdinal(i);
            HnswGraphSearcher.search(scorer, collector, graph, null);
            NeighborQueue queue = collector.queue;
            if (queue.size() == 0) {
                // no neighbors, skip
                neighborhoods[i] = NeighborHood.EMPTY;
                continue;
            }
            while (queue.size() > clustersPerNeighborhood) {
                queue.pop();
            }
            final float minScore = queue.topScore();
            final int[] neighbors = new int[queue.size()];
            for (int j = 1; j <= neighbors.length; j++) {
                neighbors[neighbors.length - j] = queue.pop();
            }
            neighborhoods[i] = new NeighborHood(neighbors, (1f / minScore) - 1);
        }
        return neighborhoods;
    }

    private static class ReusableKnnCollector implements KnnCollector {

        private final NeighborQueue queue;
        private final int k;
        int visitedCount;
        int currenOrd;

        ReusableKnnCollector(int k) {
            this.k = k;
            this.queue = new NeighborQueue(k, false);
        }

        void reset(int ord) {
            queue.clear();
            visitedCount = 0;
            currenOrd = ord;
        }

        @Override
        public boolean earlyTerminated() {
            return false;
        }

        @Override
        public void incVisitedCount(int count) {
            visitedCount += count;
        }

        @Override
        public long visitedCount() {
            return visitedCount;
        }

        @Override
        public long visitLimit() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int k() {
            return k;
        }

        @Override
        public boolean collect(int docId, float similarity) {
            if (currenOrd != docId) {
                return queue.insertWithOverflow(docId, similarity);
            }
            return false;
        }

        @Override
        public float minCompetitiveSimilarity() {
            return queue.size() >= k() ? queue.topScore() : Float.NEGATIVE_INFINITY;
        }

        @Override
        public TopDocs topDocs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public KnnSearchStrategy getSearchStrategy() {
            return null;
        }
    }
}
