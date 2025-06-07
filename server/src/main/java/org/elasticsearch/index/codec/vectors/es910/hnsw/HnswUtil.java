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

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Utilities for use in tests involving HNSW graphs */
public class HnswUtil {

    // utility class; only has static methods
    private HnswUtil() {}

    /*
     For each level, check rooted components from previous level nodes, which are entry
     points with the goal that each node should be reachable from *some* entry point.  For each entry
     point, compute a spanning tree, recording the nodes in a single shared bitset.

     Also record a bitset marking nodes that are not full to be used when reconnecting in order to
     limit the search to include non-full nodes only.
    */

    /** Returns true if every node on every level is reachable from node 0. */
    static boolean isRooted(HnswGraph knnValues) throws IOException {
        for (int level = 0; level < knnValues.numLevels(); level++) {
            if (components(knnValues, level, null, 0).size() > 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the sizes of the distinct graph components on level 0. If the graph is fully-rooted the
     * list will have one entry. If it is empty, the returned list will be empty.
     */
    static List<Integer> componentSizes(HnswGraph hnsw) throws IOException {
        return componentSizes(hnsw, 0);
    }

    /**
     * Returns the sizes of the distinct graph components on the given level. The forest starting at
     * the entry points (nodes in the next highest level) is considered as a single component. If the
     * entire graph is rooted in the entry points--that is, every node is reachable from at least one
     * entry point--the returned list will have a single entry. If the graph is empty, the returned
     * list will be empty.
     */
    static List<Integer> componentSizes(HnswGraph hnsw, int level) throws IOException {
        return components(hnsw, level, null, 0).stream().map(Component::size).toList();
    }

    // Finds orphaned components on the graph level.
    static List<Component> components(HnswGraph hnsw, int level, FixedBitSet notFullyConnected, int maxConn) throws IOException {
        List<Component> components = new ArrayList<>();
        FixedBitSet connectedNodes = new FixedBitSet(hnsw.size());
        assert hnsw.size() == hnsw.getNodesOnLevel(0).size();
        int total = 0;
        if (level >= hnsw.numLevels()) {
            throw new IllegalArgumentException("Level " + level + " too large for graph with " + hnsw.numLevels() + " levels");
        }
        HnswGraph.NodesIterator entryPoints;
        // System.out.println("components level=" + level);
        if (level == hnsw.numLevels() - 1) {
            entryPoints = new HnswGraph.ArrayNodesIterator(new int[] { hnsw.entryNode() }, 1);
        } else {
            entryPoints = hnsw.getNodesOnLevel(level + 1);
        }
        while (entryPoints.hasNext()) {
            int entryPoint = entryPoints.nextInt();
            Component component = markRooted(hnsw, level, connectedNodes, notFullyConnected, maxConn, entryPoint);
            total += component.size();
        }
        int entryPoint;
        if (notFullyConnected != null) {
            entryPoint = notFullyConnected.nextSetBit(0);
        } else {
            entryPoint = connectedNodes.nextSetBit(0);
        }
        if (total > 0) {
            components.add(new Component(entryPoint, total));
        }
        if (level == 0) {
            int nextClear = nextClearBit(connectedNodes, 0);
            while (nextClear != NO_MORE_DOCS) {
                Component component = markRooted(hnsw, level, connectedNodes, notFullyConnected, maxConn, nextClear);
                assert component.size() > 0;
                components.add(component);
                total += component.size();
                nextClear = nextClearBit(connectedNodes, component.start());
            }
        } else {
            HnswGraph.NodesIterator nodes = hnsw.getNodesOnLevel(level);
            while (nodes.hasNext()) {
                int nextClear = nodes.nextInt();
                if (connectedNodes.get(nextClear)) {
                    continue;
                }
                Component component = markRooted(hnsw, level, connectedNodes, notFullyConnected, maxConn, nextClear);
                assert component.start() == nextClear;
                assert component.size() > 0;
                components.add(component);
                total += component.size();
            }
        }
        assert total == hnsw.getNodesOnLevel(level).size()
            : "total=" + total + " level nodes on level " + level + " = " + hnsw.getNodesOnLevel(level).size();
        return components;
    }

    /**
     * Count the nodes in a rooted component of the graph and set the bits of its nodes in
     * connectedNodes bitset. Rooted means nodes that can be reached from a root node.
     *
     * @param hnswGraph the graph to check
     * @param level the level of the graph to check
     * @param connectedNodes a bitset the size of the entire graph with 1's indicating nodes that have
     *     been marked as connected. This method updates the bitset.
     * @param notFullyConnected a bitset the size of the entire graph. On output, we mark nodes
     *     visited having fewer than maxConn connections. May be null.
     * @param maxConn the maximum number of connections for any node (aka M).
     * @param entryPoint a node id to start at
     */
    private static Component markRooted(
        HnswGraph hnswGraph,
        int level,
        FixedBitSet connectedNodes,
        FixedBitSet notFullyConnected,
        int maxConn,
        int entryPoint
    ) throws IOException {
        // Start at entry point and search all nodes on this level
        // System.out.println("markRooted level=" + level + " entryPoint=" + entryPoint);
        if (connectedNodes.get(entryPoint)) {
            return new Component(entryPoint, 0);
        }
        IntHashSet nodesInStack = new IntHashSet();
        Deque<Integer> stack = new ArrayDeque<>();
        stack.push(entryPoint);
        int count = 0;
        while (stack.isEmpty() == false) {
            int node = stack.pop();
            if (connectedNodes.get(node)) {
                continue;
            }
            count++;
            connectedNodes.set(node);
            hnswGraph.seek(level, node);
            int friendOrd;
            int friendCount = 0;
            while ((friendOrd = hnswGraph.nextNeighbor()) != NO_MORE_DOCS) {
                ++friendCount;
                if (connectedNodes.get(friendOrd) == false && nodesInStack.contains(friendOrd) == false) {
                    stack.push(friendOrd);
                    nodesInStack.add(friendOrd);
                }
            }
            if (friendCount < maxConn && notFullyConnected != null) {
                notFullyConnected.set(node);
            }
        }
        return new Component(entryPoint, count);
    }

    private static int nextClearBit(FixedBitSet bits, int index) {
        // Does not depend on the ghost bits being clear!
        long[] barray = bits.getBits();
        assert index >= 0 && index < bits.length() : "index=" + index + ", numBits=" + bits.length();
        int i = index >> 6;
        long word = ~(barray[i] >> index); // skip all the bits to the right of index

        int next = NO_MORE_DOCS;
        if (word != 0) {
            next = index + Long.numberOfTrailingZeros(word);
        } else {
            while (++i < barray.length) {
                word = ~barray[i];
                if (word != 0) {
                    next = (i << 6) + Long.numberOfTrailingZeros(word);
                    break;
                }
            }
        }
        if (next >= bits.length()) {
            return NO_MORE_DOCS;
        } else {
            return next;
        }
    }

    /**
     * In graph theory, "connected components" are really defined only for undirected (ie
     * bidirectional) graphs. Our graphs are directed, because of pruning, but they are *mostly*
     * undirected. In this case we compute components starting from a single node so what we are
     * really measuring is whether the graph is a "rooted graph". TODO: measure whether the graph is
     * "strongly connected" ie there is a path from every node to every other node.
     */
    public static boolean graphIsRooted(IndexReader reader, String vectorField) throws IOException {
        for (LeafReaderContext ctx : reader.leaves()) {
            CodecReader codecReader = (CodecReader) FilterLeafReader.unwrap(ctx.reader());
            KnnVectorsReader vectorsReader = ((PerFieldKnnVectorsFormat.FieldsReader) codecReader.getVectorReader()).getFieldReader(
                vectorField
            );
            if (vectorsReader instanceof HnswGraphProvider) {
                HnswGraph graph = ((HnswGraphProvider) vectorsReader).getGraph(vectorField);
                if (isRooted(graph) == false) {
                    return false;
                }
            } else {
                throw new IllegalArgumentException("not a graph: " + vectorsReader);
            }
        }
        return true;
    }

    /**
     * A component (also "connected component") of an undirected graph is a collection of nodes that
     * are connected by neighbor links: every node in a connected component is reachable from every
     * other node in the component. See https://en.wikipedia.org/wiki/Component_(graph_theory). Such a
     * graph is said to be "fully connected" <i>iff</i> it has a single component, or it is empty.
     *
     * @param start the lowest-numbered node in the component
     * @param size the number of nodes in the component
     */
    record Component(int start, int size) {}
}
