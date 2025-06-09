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

import org.apache.lucene.util.hnsw.HnswGraph;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * This creates a graph builder that is initialized with the provided HnswGraph. This is useful for
 * merging HnswGraphs from multiple segments.
 */
public final class InitializedHnswGraphBuilder {

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
}
