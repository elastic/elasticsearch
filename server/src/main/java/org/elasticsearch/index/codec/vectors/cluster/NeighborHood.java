/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

public record NeighborHood(int[] neighbors, float maxIntraDistance) {

    static final NeighborHood EMPTY = new NeighborHood(new int[0], Float.POSITIVE_INFINITY);

    public static NeighborHood[] computeNeighborhoodsGraph(float[][] centers, int clustersPerNeighborhood) throws IOException {
        final UpdateableRandomVectorScorer scorer = new UpdateableRandomVectorScorer() {
            int scoringOrdinal;

            @Override
            public float score(int node) {
                return VectorSimilarityFunction.EUCLIDEAN.compare(centers[scoringOrdinal], centers[node]);
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
        final OnHeapHnswGraph graph = HnswGraphBuilder.create(supplier, 16, 100, 42L).build(centers.length);
        final NeighborHood[] neighborhoods = new NeighborHood[centers.length];
        final SingleBit singleBit = new SingleBit(centers.length);
        for (int i = 0; i < centers.length; i++) {
            scorer.setScoringOrdinal(i);
            singleBit.indexSet = i;
            final KnnCollector collector = HnswGraphSearcher.search(scorer, clustersPerNeighborhood, graph, singleBit, Integer.MAX_VALUE);
            final ScoreDoc[] scoreDocs = collector.topDocs().scoreDocs;
            if (scoreDocs.length == 0) {
                // no neighbors, skip
                neighborhoods[i] = NeighborHood.EMPTY;
                continue;
            }
            final int[] neighbors = new int[scoreDocs.length];
            for (int j = 0; j < neighbors.length; j++) {
                neighbors[j] = scoreDocs[j].doc;
                assert neighbors[j] != i;
            }
            final float minCompetitiveSimilarity = (1f / scoreDocs[neighbors.length - 1].score) - 1;
            neighborhoods[i] = new NeighborHood(neighbors, minCompetitiveSimilarity);
        }
        return neighborhoods;
    }

    private static class SingleBit implements Bits {

        private final int length;
        private int indexSet;

        SingleBit(int length) {
            this.length = length;
        }

        @Override
        public boolean get(int index) {
            return index != indexSet;
        }

        @Override
        public int length() {
            return length;
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
}
