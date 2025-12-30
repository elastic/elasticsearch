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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswConcurrentMergeBuilder;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public record NeighborHood(int[] neighbors, float maxIntraDistance) {

    private static final int M = 8;
    private static final int EF_CONSTRUCTION = 150;

    static final NeighborHood EMPTY = new NeighborHood(new int[0], Float.POSITIVE_INFINITY);

    public static NeighborHood[] computeNeighborhoods(float[][] centers, int clustersPerNeighborhood) throws IOException {
        assert centers.length > clustersPerNeighborhood;
        return computeNeighborhoods(null, 1, centers, clustersPerNeighborhood);
    }

    public static NeighborHood[] computeNeighborhoods(TaskExecutor executor, int numWorkers, float[][] centers, int clustersPerNeighborhood)
        throws IOException {
        assert centers.length > clustersPerNeighborhood;
        // experiments shows that below 10k, we better use brute force, otherwise hnsw gives us a nice speed up
        if (centers.length < 10_000) {
            return computeNeighborhoodsBruteForce(centers, clustersPerNeighborhood);
        } else if (executor == null || numWorkers < 2) {
            return computeNeighborhoodsGraph(centers, clustersPerNeighborhood);
        } else {
            return computeNeighborhoodsGraph(executor, numWorkers, centers, clustersPerNeighborhood);
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
        final RandomVectorScorerSupplier supplier = new CentersScorerSupplier(centers);
        final OnHeapHnswGraph graph = HnswGraphBuilder.create(supplier, M, EF_CONSTRUCTION, 42L, centers.length).build(centers.length);
        final NeighborHood[] neighborhoods = new NeighborHood[centers.length];
        populateNeighboursFromGraph(graph, clustersPerNeighborhood, neighborhoods, supplier, 0, centers.length);
        return neighborhoods;
    }

    public static NeighborHood[] computeNeighborhoodsGraph(
        TaskExecutor executor,
        int numWorkers,
        float[][] centers,
        int clustersPerNeighborhood
    ) throws IOException {
        final RandomVectorScorerSupplier supplier = new CentersScorerSupplier(centers);
        // what we want here is really is call "new OnHeapHnswGraph(M, ceneters.length)" but the constructor is package private
        final OnHeapHnswGraph initGraph = HnswGraphBuilder.create(supplier, M, EF_CONSTRUCTION, 42L, centers.length).build(0);
        final OnHeapHnswGraph graph = new HnswConcurrentMergeBuilder(executor, numWorkers, supplier, M, EF_CONSTRUCTION, initGraph, null)
            .build(centers.length);
        final NeighborHood[] neighborhoods = new NeighborHood[centers.length];
        final int len = centers.length / numWorkers;
        final List<Callable<Void>> runners = new ArrayList<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            final int start = i * len;
            final int end = i == numWorkers - 1 ? centers.length : (i + 1) * len;
            runners.add(() -> {
                populateNeighboursFromGraph(graph, clustersPerNeighborhood, neighborhoods, supplier.copy(), start, end);
                return null;
            });
        }
        executor.invokeAll(runners);
        return neighborhoods;
    }

    private static void populateNeighboursFromGraph(
        OnHeapHnswGraph graph,
        int clustersPerNeighborhood,
        NeighborHood[] neighborhoods,
        RandomVectorScorerSupplier supplier,
        int start,
        int end
    ) throws IOException {
        ReusableBits bits = new ReusableBits(graph.size());
        for (int i = start; i < end; i++) {
            supplier.scorer().setScoringOrdinal(i);
            bits.currentOrd = i;
            // oversample the number of neighbors we collect to improve recall
            final KnnCollector collector = HnswGraphSearcher.search(
                supplier.scorer(),
                2 * clustersPerNeighborhood,
                graph,
                bits,
                Integer.MAX_VALUE
            );
            ScoreDoc[] scoreDocs = collector.topDocs().scoreDocs;
            int len = Math.min(clustersPerNeighborhood, scoreDocs.length);
            if (len == 0) {
                // no neighbors, skip
                neighborhoods[i] = NeighborHood.EMPTY;
                continue;
            }
            final float minScore = scoreDocs[len - 1].score;
            final int[] neighbors = new int[len];
            for (int j = 0; j < len; j++) {
                neighbors[j] = scoreDocs[j].doc;
            }
            neighborhoods[i] = new NeighborHood(neighbors, (1f / minScore) - 1);
        }
    }

    private static class ReusableBits implements Bits {

        final int size;
        int currentOrd;

        ReusableBits(int size) {
            this.size = size;
        }

        @Override
        public boolean get(int index) {
            return index != currentOrd;
        }

        @Override
        public int length() {
            return size;
        }
    }

    private record CentersScorerSupplier(float[][] centers, UpdateableRandomVectorScorer scorer) implements RandomVectorScorerSupplier {

        CentersScorerSupplier(float[][] centers) {
            this(centers, new UpdateableRandomVectorScorer() {
                private int scoringOrdinal;
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
            });
        }

        @Override
        public UpdateableRandomVectorScorer scorer() {
            return scorer;
        }

        @Override
        public RandomVectorScorerSupplier copy() {
            return new CentersScorerSupplier(centers);
        }
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
