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
package org.elasticsearch.index.codec.vectors.es910.hnsw;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Searches an HNSW graph to find nearest neighbors to a query vector. For more background on the
 * search algorithm, see {@link HnswGraph}.
 */
public class HnswGraphSearcher extends AbstractHnswGraphSearcher {
    /**
     * Scratch data structures that are used in each {@link #searchLevel} call. These can be expensive
     * to allocate, so they're cleared and reused across calls.
     */
    protected final NeighborQueue candidates;

    protected BitSet visited;

    /**
     * Creates a new graph searcher.
     *
     * @param candidates max heap that will track the candidate nodes to explore
     * @param visited bit set that will track nodes that have already been visited
     */
    public HnswGraphSearcher(NeighborQueue candidates, BitSet visited) {
        this.candidates = candidates;
        this.visited = visited;
    }

    /**
     * Function to find the best entry point from which to search the zeroth graph layer.
     *
     * @param scorer the scorer to compare the query with the nodes
     * @param graph the HNSWGraph
     * @param collector the knn result collector
     * @return the best entry point, `-1` indicates graph entry node not set, or visitation limit
     *     exceeded
     * @throws IOException When accessing the vector fails
     */
    @Override
    int[] findBestEntryPoint(RandomVectorScorer scorer, HnswGraph graph, KnnCollector collector) throws IOException {
        int currentEp = graph.entryNode();
        if (currentEp == -1 || graph.numLevels() == 1) {
            return new int[] { currentEp };
        }
        int size = getGraphSize(graph);
        prepareScratchState(size);
        float currentScore = scorer.score(currentEp);
        collector.incVisitedCount(1);
        boolean foundBetter;
        for (int level = graph.numLevels() - 1; level >= 1; level--) {
            foundBetter = true;
            visited.set(currentEp);
            // Keep searching the given level until we stop finding a better candidate entry point
            while (foundBetter) {
                foundBetter = false;
                graphSeek(graph, level, currentEp);
                int friendOrd;
                while ((friendOrd = graphNextNeighbor(graph)) != NO_MORE_DOCS) {
                    assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
                    if (visited.getAndSet(friendOrd)) {
                        continue;
                    }
                    if (collector.earlyTerminated()) {
                        return new int[] { UNK_EP };
                    }
                    float friendSimilarity = scorer.score(friendOrd);
                    collector.incVisitedCount(1);
                    if (friendSimilarity > currentScore) {
                        currentScore = friendSimilarity;
                        currentEp = friendOrd;
                        foundBetter = true;
                    }
                }
            }
        }
        return collector.earlyTerminated() ? new int[] { UNK_EP } : new int[] { currentEp };
    }

    /**
     * Add the closest neighbors found to a priority queue (heap). These are returned in REVERSE
     * proximity order -- the most distant neighbor of the topK found, i.e. the one with the lowest
     * score/comparison value, will be at the top of the heap, while the closest neighbor will be the
     * last to be popped.
     */
    @Override
    void searchLevel(KnnCollector results, RandomVectorScorer scorer, int level, final int[] eps, HnswGraph graph, Bits acceptOrds)
        throws IOException {

        int size = getGraphSize(graph);

        prepareScratchState(size);

        for (int ep : eps) {
            if (visited.getAndSet(ep) == false) {
                if (results.earlyTerminated()) {
                    break;
                }
                float score = scorer.score(ep);
                results.incVisitedCount(1);
                candidates.add(ep, score);
                if (acceptOrds == null || acceptOrds.get(ep)) {
                    results.collect(ep, score);
                }
            }
        }

        // A bound that holds the minimum similarity to the query vector that a candidate vector must
        // have to be considered.
        float minAcceptedSimilarity = Math.nextUp(results.minCompetitiveSimilarity());
        // We should allow exploring equivalent minAcceptedSimilarity values at least once
        boolean shouldExploreMinSim = true;
        while (candidates.size() > 0 && results.earlyTerminated() == false) {
            // get the best candidate (closest or best scoring)
            float topCandidateSimilarity = candidates.topScore();
            if (topCandidateSimilarity < minAcceptedSimilarity) {
                // if the similarity is equivalent to the minAcceptedSimilarity,
                // we should explore one candidate
                // however, running into many duplicates can be expensive,
                // so we should stop exploring if equivalent minimum scores are found
                if (shouldExploreMinSim && Math.nextUp(topCandidateSimilarity) == minAcceptedSimilarity) {
                    shouldExploreMinSim = false;
                } else {
                    break;
                }
            }

            int topCandidateNode = candidates.pop();
            graphSeek(graph, level, topCandidateNode);
            int friendOrd;
            while ((friendOrd = graphNextNeighbor(graph)) != NO_MORE_DOCS) {
                assert friendOrd < size : "friendOrd=" + friendOrd + "; size=" + size;
                if (visited.getAndSet(friendOrd)) {
                    continue;
                }

                if (results.earlyTerminated()) {
                    break;
                }
                float friendSimilarity = scorer.score(friendOrd);
                results.incVisitedCount(1);
                if (friendSimilarity >= minAcceptedSimilarity) {
                    candidates.add(friendOrd, friendSimilarity);
                    if (acceptOrds == null || acceptOrds.get(friendOrd)) {
                        if (results.collect(friendOrd, friendSimilarity)) {
                            float oldMinAcceptedSimilarity = minAcceptedSimilarity;
                            minAcceptedSimilarity = Math.nextUp(results.minCompetitiveSimilarity());
                            if (minAcceptedSimilarity > oldMinAcceptedSimilarity) {
                                // we adjusted our minAcceptedSimilarity, so we should explore the next equivalent
                                // if necessary
                                shouldExploreMinSim = true;
                            }
                        }
                    }
                }
            }
            if (results.getSearchStrategy() != null) {
                results.getSearchStrategy().nextVectorsBlock();
            }
        }
    }

    private void prepareScratchState(int capacity) {
        candidates.clear();
        if (visited.length() < capacity) {
            visited = FixedBitSet.ensureCapacity((FixedBitSet) visited, capacity);
        }
        visited.clear();
    }

    /**
     * Seek a specific node in the given graph. The default implementation will just call {@link
     * HnswGraph#seek(int, int)}
     *
     * @throws IOException when seeking the graph
     */
    void graphSeek(HnswGraph graph, int level, int targetNode) throws IOException {
        graph.seek(level, targetNode);
    }

    /**
     * Get the next neighbor from the graph, you must call {@link #graphSeek(HnswGraph, int, int)}
     * before calling this method. The default implementation will just call {@link
     * HnswGraph#nextNeighbor()}
     *
     * @return see {@link HnswGraph#nextNeighbor()}
     * @throws IOException when advance neighbors
     */
    int graphNextNeighbor(HnswGraph graph) throws IOException {
        return graph.nextNeighbor();
    }

    static int getGraphSize(HnswGraph graph) {
        return graph.maxNodeId() + 1;
    }
}
