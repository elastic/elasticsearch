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
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.GroupVIntUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.HnswGraph;

import java.io.IOException;
import java.util.Arrays;

/**
 * Off-heap {@link HnswGraph} that reads neighbor lists directly from an {@link IndexInput} slice
 * rather than keeping them in memory. The serialized format is produced by
 * {@link HnswUtils#writeMultiLevelGraph}.
 * The only minor difference from Lucene's is that GroupVarInt is always utilized for neighbor connections.
 */
public final class OffHeapHnswGraph extends HnswGraph {
    private final IndexInput neighborData;
    private final int[][] nodesByLevel;
    private final LongValues offsets;
    private final long[] levelIndexOffsets;
    private final int size;
    private final int numLevels;
    private final int entryNode;
    private final int maxConn;
    private final int[] currentNeighbors;
    private int arcCount;
    private int arcUpTo;

    OffHeapHnswGraph(
        IndexInput neighborData,
        int[][] nodesByLevel,
        LongValues offsets,
        int size,
        int numLevels,
        int entryNode,
        int maxConn
    ) {
        this.neighborData = neighborData;
        this.nodesByLevel = nodesByLevel;
        this.offsets = offsets;
        this.size = size;
        this.numLevels = numLevels;
        this.entryNode = entryNode;
        this.maxConn = maxConn;
        this.currentNeighbors = new int[maxConn * 2];
        this.levelIndexOffsets = new long[numLevels];
        for (int level = 1; level < numLevels; level++) {
            int lowerCount = nodesByLevel[level - 1] == null ? size : nodesByLevel[level - 1].length;
            levelIndexOffsets[level] = levelIndexOffsets[level - 1] + lowerCount;
        }
    }

    @Override
    public void seek(int level, int target) throws IOException {
        final int index = level == 0 ? target : Arrays.binarySearch(nodesByLevel[level], 0, nodesByLevel[level].length, target);
        assert index >= 0 : "seek level=" + level + " target=" + target + " not found";
        neighborData.seek(offsets.get(index + levelIndexOffsets[level]));
        arcCount = neighborData.readVInt();
        assert arcCount <= currentNeighbors.length : "too many neighbors: " + arcCount;
        if (arcCount > 0) {
            GroupVIntUtil.readGroupVInts(neighborData, currentNeighbors, arcCount);
            int sum = 0;
            for (int i = 0; i < arcCount; i++) {
                sum += currentNeighbors[i];
                currentNeighbors[i] = sum;
            }
        }
        arcUpTo = 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int nextNeighbor() {
        if (arcUpTo >= arcCount) {
            return DocIdSetIterator.NO_MORE_DOCS;
        }
        return currentNeighbors[arcUpTo++];
    }

    @Override
    public int numLevels() {
        return numLevels;
    }

    @Override
    public int maxConn() {
        return maxConn;
    }

    @Override
    public int entryNode() {
        return entryNode;
    }

    @Override
    public int neighborCount() {
        return arcCount;
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
        if (level == 0) {
            return new DenseNodesIterator(size);
        }
        return new ArrayNodesIterator(nodesByLevel[level], nodesByLevel[level].length);
    }
}
