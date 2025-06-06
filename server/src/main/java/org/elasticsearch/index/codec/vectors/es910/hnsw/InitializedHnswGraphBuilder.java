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

import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * This creates a graph builder that is initialized with the provided HnswGraph. This is useful for
 * merging HnswGraphs from multiple segments.
 */
public final class InitializedHnswGraphBuilder extends HnswGraphBuilder {

    /**
     * Create a new HnswGraphBuilder that is initialized with the provided HnswGraph.
     *
     * @param scorerSupplier the scorer to use for vectors
     * @param beamWidth the number of nodes to explore in the search
     * @param seed the seed for the random number generator
     * @param initializerGraph the graph to initialize the new graph builder
     * @param newOrdMap a mapping from the old node ordinal to the new node ordinal
     * @param initializedNodes a bitset of nodes that are already initialized in the initializerGraph
     * @param totalNumberOfVectors the total number of vectors in the new graph, this should include
     *     all vectors expected to be added to the graph in the future
     * @return a new HnswGraphBuilder that is initialized with the provided HnswGraph
     * @throws IOException when reading the graph fails
     */
    public static InitializedHnswGraphBuilder fromGraph(
        RandomVectorScorerSupplier scorerSupplier,
        int beamWidth,
        long seed,
        HnswGraph initializerGraph,
        int[] newOrdMap,
        BitSet initializedNodes,
        int totalNumberOfVectors
    ) throws IOException {
        return new InitializedHnswGraphBuilder(
            scorerSupplier,
            beamWidth,
            seed,
            initGraph(initializerGraph, newOrdMap, totalNumberOfVectors),
            initializedNodes
        );
    }

    public static OnHeapHnswGraph initGraph(HnswGraph initializerGraph, int[] newOrdMap, int totalNumberOfVectors) throws IOException {
        OnHeapHnswGraph hnsw = new OnHeapHnswGraph(initializerGraph.maxConn(), totalNumberOfVectors);
        for (int level = initializerGraph.numLevels() - 1; level >= 0; level--) {
            HnswGraph.NodesIterator it = initializerGraph.getNodesOnLevel(level);
            while (it.hasNext()) {
                int oldOrd = it.nextInt();
                int newOrd = newOrdMap[oldOrd];
                hnsw.addNode(level, newOrd);
                hnsw.trySetNewEntryNode(newOrd, level);
                NeighborArray newNeighbors = hnsw.getNeighbors(level, newOrd);
                initializerGraph.seek(level, oldOrd);
                for (int oldNeighbor = initializerGraph.nextNeighbor(); oldNeighbor != NO_MORE_DOCS; oldNeighbor = initializerGraph
                    .nextNeighbor()) {
                    int newNeighbor = newOrdMap[oldNeighbor];
                    // we will compute these scores later when we need to pop out the non-diverse nodes
                    newNeighbors.addOutOfOrder(newNeighbor, Float.NaN);
                }
            }
        }
        return hnsw;
    }

    private final BitSet initializedNodes;

    public InitializedHnswGraphBuilder(
        RandomVectorScorerSupplier scorerSupplier,
        int beamWidth,
        long seed,
        OnHeapHnswGraph initializedGraph,
        BitSet initializedNodes
    ) throws IOException {
        super(scorerSupplier, beamWidth, seed, initializedGraph);
        this.initializedNodes = initializedNodes;
    }

    @Override
    public void addGraphNode(int node, UpdateableRandomVectorScorer scorer) throws IOException {
        if (initializedNodes.get(node)) {
            return;
        }
        super.addGraphNode(node, scorer);
    }

    @Override
    public void addGraphNode(int node) throws IOException {
        if (initializedNodes.get(node)) {
            return;
        }
        super.addGraphNode(node);
    }
}
