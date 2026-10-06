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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswConcurrentMergeBuilder;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Contains an array of the nearest centroid to a specific centroid.
 *
 * @param neighbors        ordinals of the nearest neighboring centers in distance order
 * @param maxIntraDistance the squared distance from this center to the farthest center in {@code neighbors}.
 *                         This can be used as a bound when finding the closest center to a vector.
 */
public record NeighborHood(int[] neighbors, float maxIntraDistance) {

    private static final int M = 8;
    private static final int EF_CONSTRUCTION = 150;

    static final NeighborHood EMPTY = new NeighborHood(new int[0], Float.POSITIVE_INFINITY);

    /**
     * Computes the neighborhood for each centroid in {@code centers}.
     *
     * @param ops          the centroid operations
     * @param centers      the centroids
     * @param clustersPerNeighborhood the maximum number of nearest neighbors to compute for each centroid
     * @return the neighborhoods for each centroid, corresponding to the input centroids in {@code centers}
     */
    public static <V> NeighborHood[] computeNeighborhoods(CentroidOps<V> ops, V[] centers, int clustersPerNeighborhood) throws IOException {
        return computeNeighborhoods(ops, null, 1, centers, clustersPerNeighborhood);
    }

    /**
     * Computes the neighborhood for each centroid in {@code centers}.
     *
     * @param ops          the centroid operations
     * @param executor     the task executor, or null to use a single thread
     * @param numWorkers   the number of workers to use
     * @param centers      the centroids
     * @param clustersPerNeighborhood the maximum number of nearest neighbors to compute for each centroid
     * @return the neighborhoods for each centroid, corresponding to the input centroids in {@code centers}
     */
    public static <V> NeighborHood[] computeNeighborhoods(
        CentroidOps<V> ops,
        @Nullable TaskExecutor executor,
        int numWorkers,
        V[] centers,
        int clustersPerNeighborhood
    ) throws IOException {
        assert centers.length > clustersPerNeighborhood;
        // experiments shows that below 10k, we better use brute force, otherwise hnsw gives us a nice speed up
        if (centers.length < 10_000) {
            return computeNeighborhoodsBruteForce(ops, centers, clustersPerNeighborhood);
        } else if (executor == null || numWorkers < 2) {
            return computeNeighborhoodsGraph(ops, centers, clustersPerNeighborhood);
        } else {
            return computeNeighborhoodsGraph(ops, executor, numWorkers, centers, clustersPerNeighborhood);
        }
    }

    public static <V> NeighborHood[] computeNeighborhoodsBruteForce(CentroidOps<V> ops, V[] centers, int clustersPerNeighborhood) {
        int k = centers.length;
        NeighborQueue[] neighborQueues = new NeighborQueue[k];
        for (int i = 0; i < k; i++) {
            neighborQueues[i] = new NeighborQueue(clustersPerNeighborhood, true);
        }
        final float[] scores = new float[4];
        final int limit = k - 3;
        for (int i = 0; i < k - 1; i++) {
            V center = centers[i];
            int j = i + 1;
            for (; j < limit; j += 4) {
                ops.squareDistanceBulk(center, centers[j], centers[j + 1], centers[j + 2], centers[j + 3], 0, scores);
                for (int h = 0; h < 4; h++) {
                    neighborQueues[j + h].insertWithOverflow(i, scores[h]);
                    neighborQueues[i].insertWithOverflow(j + h, scores[h]);
                }
            }
            for (; j < k; j++) {
                float dsq = ops.squareDistance(center, centers[j]);
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

    public static <V> NeighborHood[] computeNeighborhoodsGraph(CentroidOps<V> ops, V[] centers, int clustersPerNeighborhood)
        throws IOException {
        final RandomVectorScorerSupplier supplier = new CentersScorerSupplier<>(ops, centers);
        final OnHeapHnswGraph graph = HnswGraphBuilder.create(supplier, M, EF_CONSTRUCTION, 42L, centers.length).build(centers.length);
        final NeighborHood[] neighborhoods = new NeighborHood[centers.length];
        populateNeighboursFromGraph(graph, clustersPerNeighborhood, neighborhoods, supplier, 0, centers.length);
        return neighborhoods;
    }

    public static <V> NeighborHood[] computeNeighborhoodsGraph(
        CentroidOps<V> ops,
        TaskExecutor executor,
        int numWorkers,
        V[] centers,
        int clustersPerNeighborhood
    ) throws IOException {
        final RandomVectorScorerSupplier supplier = new CentersScorerSupplier<>(ops, centers);
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

    private record CentersScorerSupplier<V>(CentroidOps<V> ops, V[] centers, UpdateableRandomVectorScorer scorer)
        implements
            RandomVectorScorerSupplier {

        CentersScorerSupplier(CentroidOps<V> ops, V[] centers) {
            this(ops, centers, new UpdateableRandomVectorScorer() {
                private int scoringOrdinal;
                private final float[] distances = new float[4];

                @Override
                public float score(int node) {
                    return VectorUtil.normalizeDistanceToUnitInterval(ops.squareDistance(centers[scoringOrdinal], centers[node]));
                }

                @Override
                public float bulkScore(int[] nodes, float[] scores, int numNodes) {
                    int i = 0;
                    final int limit = numNodes - 3;
                    float max = Float.NEGATIVE_INFINITY;
                    for (; i < limit; i += 4) {
                        ops.squareDistanceBulk(
                            centers[scoringOrdinal],
                            centers[nodes[i]],
                            centers[nodes[i + 1]],
                            centers[nodes[i + 2]],
                            centers[nodes[i + 3]],
                            0,
                            distances
                        );
                        for (int j = 0; j < 4; j++) {
                            scores[i + j] = VectorUtil.normalizeDistanceToUnitInterval(distances[j]);
                            max = Math.max(max, scores[i + j]);
                        }
                    }
                    for (; i < numNodes; i++) {
                        scores[i] = score(nodes[i]);
                        max = Math.max(max, scores[i]);
                    }
                    return max;
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
            return new CentersScorerSupplier<>(ops, centers);
        }
    }
}
