/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es819.hnsw;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Searches an HNSW graph to find nearest neighbors to a query vector. This particular
 * implementation is optimized for a filtered search, inspired by the ACORN-1 algorithm.
 * https://arxiv.org/abs/2403.04871 However, this implementation is augmented in some ways, mainly:
 *
 * <ul>
 *   <li>It dynamically determines when the optimized filter step should occur based on some
 *       filtered lambda. This is done per small world
 *   <li>The graph searcher doesn't always explore all the extended neighborhood and the number of
 *       additional candidates is predicated on the original candidate's filtered percentage.
 * </ul>
 */
public class FilteredHnswGraphSearcher extends HnswGraphSearcher {
    // How many filtered candidates must be found to consider N-hop neighbors
    private static final float EXPANDED_EXPLORATION_LAMBDA = 0.10f;

    // How many extra neighbors to explore, used as a multiple to the candidates neighbor count
    private final int maxExplorationMultiplier;
    private final int minToScore;

    /** Creates a new graph searcher. */
    private FilteredHnswGraphSearcher(NeighborQueue candidates, BitSet visited, int filterSize, HnswGraph graph) {
        super(candidates, visited);
        assert graph.maxConn() > 0 : "graph must have known max connections";
        float filterRatio = filterSize / (float) graph.size();
        this.maxExplorationMultiplier = (int) Math.round(Math.min(1 / filterRatio, graph.maxConn() / 2.0));
        // As the filter gets exceptionally restrictive, we must spread out the exploration
        this.minToScore = (int) Math.round(Math.min(Math.max(0, 1.0 / filterRatio - (2.0 * graph.maxConn())), graph.maxConn()));
    }

    /**
     * Creates a new filtered graph searcher.
     *
     * @param k the number of nearest neighbors to find
     * @param graph the graph to search
     * @param filterSize the number of vectors that pass the accepted ords filter
     * @param acceptOrds the accepted ords filter
     * @return a new graph searcher optimized for filtered search
     */
    public static FilteredHnswGraphSearcher create(int k, HnswGraph graph, int filterSize, Bits acceptOrds) {
        if (acceptOrds == null) {
            throw new IllegalArgumentException("acceptOrds must not be null to used filtered search");
        }
        if (filterSize <= 0 || filterSize >= getGraphSize(graph)) {
            throw new IllegalArgumentException("filterSize must be > 0 and < graph size");
        }
        return new FilteredHnswGraphSearcher(new NeighborQueue(k, true), bitSet(filterSize, getGraphSize(graph), k), filterSize, graph);
    }

    private static BitSet bitSet(long filterSize, int graphSize, int topk) {
        float percentFiltered = (float) filterSize / graphSize;
        assert percentFiltered > 0.0f && percentFiltered < 1.0f;
        double totalOps = Math.log(graphSize) * topk;
        int approximateVisitation = (int) (totalOps / percentFiltered);
        return bitSet(approximateVisitation, graphSize);
    }

    private static BitSet bitSet(int expectedBits, int totalBits) {
        if (expectedBits < (totalBits >>> 7)) {
            return new SparseFixedBitSet(totalBits);
        } else {
            return new FixedBitSet(totalBits);
        }
    }

    @Override
    void searchLevel(KnnCollector results, RandomVectorScorer scorer, int level, final int[] eps, HnswGraph graph, Bits acceptOrds)
        throws IOException {
        assert level == 0 : "Filtered search only works on the base level";

        int size = getGraphSize(graph);

        prepareScratchState();

        for (int ep : eps) {
            if (visited.getAndSet(ep) == false) {
                if (results.earlyTerminated()) {
                    return;
                }
                float score = scorer.score(ep);
                results.incVisitedCount(1);
                candidates.add(ep, score);
                if (acceptOrds.get(ep)) {
                    results.collect(ep, score);
                }
            }
        }
        // Collect the vectors to score and potentially add as candidates
        IntArrayQueue toScore = new IntArrayQueue(graph.maxConn() * 2 * maxExplorationMultiplier);
        IntArrayQueue toExplore = new IntArrayQueue(graph.maxConn() * 2 * maxExplorationMultiplier);
        // A bound that holds the minimum similarity to the query vector that a candidate vector must
        // have to be considered.
        float minAcceptedSimilarity = Math.nextUp(results.minCompetitiveSimilarity());
        while (candidates.size() > 0 && results.earlyTerminated() == false) {
            // get the best candidate (closest or best scoring)
            float topCandidateSimilarity = candidates.topScore();
            if (minAcceptedSimilarity > topCandidateSimilarity) {
                break;
            }
            int topCandidateNode = candidates.pop();
            graph.seek(level, topCandidateNode);
            int neighborCount = graph.neighborCount();
            toScore.clear();
            toExplore.clear();
            int friendOrd;
            while ((friendOrd = graph.nextNeighbor()) != NO_MORE_DOCS && toScore.isFull() == false) {
                assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
                if (visited.getAndSet(friendOrd)) {
                    continue;
                }
                if (acceptOrds.get(friendOrd)) {
                    toScore.add(friendOrd);
                } else {
                    toExplore.add(friendOrd);
                }
            }
            // adjust to locally number of filtered candidates to explore
            float filteredAmount = toExplore.count() / (float) neighborCount;
            int maxToScoreCount = (int) (neighborCount * Math.min(maxExplorationMultiplier, 1f / (1f - filteredAmount)));
            int maxAdditionalToExploreCount = toExplore.capacity() - 1;
            // There is enough filtered, or we don't have enough candidates to score and explore
            int totalExplored = toScore.count() + toExplore.count();
            if (toScore.count() < maxToScoreCount && filteredAmount > EXPANDED_EXPLORATION_LAMBDA) {
                // Now we need to explore the neighbors of the neighbors
                int exploreFriend;
                while ((exploreFriend = toExplore.poll()) != NO_MORE_DOCS
                    // only explore initial additional neighborhood
                    && totalExplored < maxAdditionalToExploreCount
                    && toScore.count() < maxToScoreCount) {
                    graphSeek(graph, level, exploreFriend);
                    int friendOfAFriendOrd;
                    while ((friendOfAFriendOrd = graph.nextNeighbor()) != NO_MORE_DOCS && toScore.count() < maxToScoreCount) {
                        if (visited.getAndSet(friendOfAFriendOrd)) {
                            continue;
                        }
                        totalExplored++;
                        if (acceptOrds.get(friendOfAFriendOrd)) {
                            toScore.add(friendOfAFriendOrd);
                            // If we have YET to find a minimum of number candidates, we will continue to explore
                            // until our max
                        } else if (totalExplored < maxAdditionalToExploreCount && toScore.count() < minToScore) {
                            toExplore.add(friendOfAFriendOrd);
                        }
                    }
                }
            }
            // Score the vectors and add them to the candidate list
            int toScoreOrd;
            while ((toScoreOrd = toScore.poll()) != NO_MORE_DOCS) {
                float friendSimilarity = scorer.score(toScoreOrd);
                results.incVisitedCount(1);
                if (friendSimilarity > minAcceptedSimilarity) {
                    candidates.add(toScoreOrd, friendSimilarity);
                    if (results.collect(toScoreOrd, friendSimilarity)) {
                        minAcceptedSimilarity = Math.nextUp(results.minCompetitiveSimilarity());
                    }
                }
            }
            if (results.getSearchStrategy() != null) {
                results.getSearchStrategy().nextVectorsBlock();
            }
        }
    }

    private void prepareScratchState() {
        candidates.clear();
        visited.clear();
    }

    private static class IntArrayQueue {
        private int[] nodes;
        private int upto;
        private int size;

        IntArrayQueue(int capacity) {
            nodes = new int[capacity];
        }

        int capacity() {
            return nodes.length;
        }

        int count() {
            return size - upto;
        }

        void add(int node) {
            if (isFull()) {
                throw new UnsupportedOperationException("Initial capacity should remain unchanged");
            }
            nodes[size++] = node;
        }

        boolean isFull() {
            return size == nodes.length;
        }

        int poll() {
            if (upto == size) {
                return NO_MORE_DOCS;
            }
            return nodes[upto++];
        }

        void clear() {
            upto = 0;
            size = 0;
        }
    }
}
