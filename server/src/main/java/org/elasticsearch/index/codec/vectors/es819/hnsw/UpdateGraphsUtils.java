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

import org.apache.lucene.util.LongHeap;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Utility class for updating a big graph with smaller graphs. This is used during merging of
 * segments containing HNSW graphs.
 */
public class UpdateGraphsUtils {

    /**
     * Find nodes in the graph that best cover the graph. This is reminiscent of an edge cover
     * problem. Here rather than choosing edges we pick nodes and increment a count at their
     * neighbours.
     *
     * @return a set of nodes that best cover the graph
     */
    public static Set<Integer> computeJoinSet(HnswGraph graph) throws IOException {
        int k; // coverage for the current node
        int size = graph.size();
        LongHeap heap = new LongHeap(size);
        Set<Integer> j = new HashSet<>();
        boolean[] stale = new boolean[size];
        short[] counts = new short[size];
        long gExit = 0L;
        for (int v = 0; v < size; v++) {
            graph.seek(0, v);
            int degree = graph.neighborCount();
            k = degree < 9 ? 2 : Math.ceilDiv(degree, 4);
            gExit += k;
            int gain = k + degree;
            heap.push(encode(gain, v));
        }

        long gTot = 0L;
        while (gTot < gExit && heap.size() > 0) {
            long el = heap.pop();
            int gain = decodeValue1(el);
            int v = decodeValue2(el);
            graph.seek(0, v);
            int degree = graph.neighborCount();
            int[] ns = new int[degree];
            int i = 0;
            for (int u = graph.nextNeighbor(); u != NO_MORE_DOCS; u = graph.nextNeighbor()) {
                ns[i++] = u;
            }
            k = degree < 9 ? 2 : Math.ceilDiv(degree, 4);
            if (stale[v]) { // if stale, recalculate gain
                int newGain = Math.max(0, k - counts[v]);
                for (int u : ns) {
                    if (counts[u] < k && j.contains(u) == false) {
                        newGain += 1;
                    }
                }
                if (newGain > 0) {
                    heap.push(encode(newGain, v));
                    stale[v] = false;
                }
            } else {
                j.add(v);
                gTot += gain;
                boolean markNeighboursStale = counts[v] < k;
                for (int u : ns) {
                    if (markNeighboursStale) {
                        stale[u] = true;
                    }
                    if (counts[u] < (k - 1)) {
                        // make neighbours of u stale
                        graph.seek(0, u);
                        for (int uu = graph.nextNeighbor(); uu != NO_MORE_DOCS; uu = graph.nextNeighbor()) {
                            stale[uu] = true;
                        }
                    }
                    counts[u] += 1;
                }
            }
        }
        return j;
    }

    private static long encode(int value1, int value2) {
        return (((long) -value1) << 32) | (value2 & 0xFFFFFFFFL);
    }

    private static int decodeValue1(long encoded) {
        return (int) -(encoded >> 32);
    }

    private static int decodeValue2(long encoded) {
        return (int) (encoded & 0xFFFFFFFFL);
    }
}
