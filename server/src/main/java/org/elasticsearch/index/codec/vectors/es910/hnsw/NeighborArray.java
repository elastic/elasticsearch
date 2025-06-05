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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.index.codec.vectors.es910.internal.hppc.MaxSizedFloatArrayList;
import org.elasticsearch.index.codec.vectors.es910.internal.hppc.MaxSizedIntArrayList;

import java.io.IOException;
import java.util.Arrays;

/**
 * NeighborArray encodes the neighbors of a node and their mutual scores in the HNSW graph as a pair
 * of growable arrays. Nodes are arranged in the sorted order of their scores in descending order
 * (if scoresDescOrder is true), or in the ascending order of their scores (if scoresDescOrder is
 * false)
 *
 * @lucene.internal
 */
public class NeighborArray implements Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(NeighborArray.class);

    private final boolean scoresDescOrder;
    private int size;
    private final int maxSize;
    private final MaxSizedFloatArrayList scores;
    private final MaxSizedIntArrayList nodes;
    private int sortedNodeSize;

    public NeighborArray(int maxSize, boolean descOrder) {
        this.maxSize = maxSize;
        nodes = new MaxSizedIntArrayList(maxSize, maxSize / 8);
        scores = new MaxSizedFloatArrayList(maxSize, maxSize / 8);
        this.scoresDescOrder = descOrder;
    }

    /**
     * Add a new node to the NeighborArray. The new node must be worse than all previously stored
     * nodes. This cannot be called after {@link #addOutOfOrder(int, float)}
     */
    public void addInOrder(int newNode, float newScore) {
        assert size == sortedNodeSize : "cannot call addInOrder after addOutOfOrder";
        if (size == maxSize) {
            throw new IllegalStateException("No growth is allowed");
        }
        if (size > 0) {
            float previousScore = scores.get(size - 1);
            assert ((scoresDescOrder && (previousScore >= newScore)) || (scoresDescOrder == false && (previousScore <= newScore)))
                : "Nodes are added in the incorrect order! Comparing " + newScore + " to " + Arrays.toString(scores.toArray());
        }
        nodes.add(newNode);
        scores.add(newScore);
        ++size;
        ++sortedNodeSize;
    }

    /** Add node and newScore but do not insert as sorted */
    public void addOutOfOrder(int newNode, float newScore) {
        if (size == maxSize) {
            throw new IllegalStateException("No growth is allowed");
        }

        nodes.add(newNode);
        scores.add(newScore);
        size++;
    }

    /**
     * In addition to {@link #addOutOfOrder(int, float)}, this function will also remove the
     * least-diverse node if the node array is full after insertion
     *
     * <p>In multi-threading environment, this method need to be locked as it will be called by
     * multiple threads while other add method is only supposed to be called by one thread.
     *
     * @param nodeId node Id of the owner of this NeighbourArray
     */
    public void addAndEnsureDiversity(int newNode, float newScore, int nodeId, UpdateableRandomVectorScorer scorer) throws IOException {
        addOutOfOrder(newNode, newScore);
        if (size < maxSize) {
            return;
        }
        // we're oversize, need to do diversity check and pop out the least diverse neighbour
        scorer.setScoringOrdinal(nodeId);
        removeIndex(findWorstNonDiverse(scorer));
        assert size == maxSize - 1;
    }

    /**
     * Sort the array according to scores, and return the sorted indexes of previous unsorted nodes
     * (unchecked nodes)
     *
     * @return indexes of newly sorted (unchecked) nodes, in ascending order, or null if the array is
     *     already fully sorted
     */
    int[] sort(RandomVectorScorer scorer) throws IOException {
        if (size == sortedNodeSize) {
            // all nodes checked and sorted
            return null;
        }
        assert sortedNodeSize < size;
        int[] uncheckedIndexes = new int[size - sortedNodeSize];
        int count = 0;
        while (sortedNodeSize != size) {
            // TODO: Instead of do an array copy on every insertion, I think we can do better here:
            // Remember the insertion point of each unsorted node and insert them altogether
            // We can save several array copy by doing that
            uncheckedIndexes[count] = insertSortedInternal(scorer); // sortedNodeSize is increased inside
            for (int i = 0; i < count; i++) {
                if (uncheckedIndexes[i] >= uncheckedIndexes[count]) {
                    // the previous inserted nodes has been shifted
                    uncheckedIndexes[i]++;
                }
            }
            count++;
        }
        Arrays.sort(uncheckedIndexes);
        return uncheckedIndexes;
    }

    /** insert the first unsorted node into its sorted position */
    private int insertSortedInternal(RandomVectorScorer scorer) throws IOException {
        assert sortedNodeSize < size : "Call this method only when there's unsorted node";
        int tmpNode = nodes.get(sortedNodeSize);
        float tmpScore = scores.get(sortedNodeSize);

        if (Float.isNaN(tmpScore)) {
            tmpScore = scorer.score(tmpNode);
        }

        int insertionPoint = scoresDescOrder
            ? descSortFindRightMostInsertionPoint(tmpScore, sortedNodeSize)
            : ascSortFindRightMostInsertionPoint(tmpScore, sortedNodeSize);
        System.arraycopy(nodes.buffer, insertionPoint, nodes.buffer, insertionPoint + 1, sortedNodeSize - insertionPoint);
        System.arraycopy(scores.buffer, insertionPoint, scores.buffer, insertionPoint + 1, sortedNodeSize - insertionPoint);
        nodes.buffer[insertionPoint] = tmpNode;
        scores.buffer[insertionPoint] = tmpScore;
        ++sortedNodeSize;
        return insertionPoint;
    }

    /** This method is for test only. */
    void insertSorted(int newNode, float newScore) throws IOException {
        addOutOfOrder(newNode, newScore);
        insertSortedInternal(null);
    }

    public int size() {
        return size;
    }

    /**
     * Direct access to the internal list of node ids; provided for efficient writing of the graph
     *
     * @lucene.internal
     */
    public int[] nodes() {
        return nodes.buffer;
    }

    /**
     * Get the score at the given index
     *
     * @param i index of the score to get
     * @return the score at the given index
     */
    public float getScores(int i) {
        return scores.get(i);
    }

    public void clear() {
        size = 0;
        sortedNodeSize = 0;
        nodes.clear();
        scores.clear();
    }

    void removeLast() {
        nodes.removeLast();
        scores.removeLast();
        size--;
        sortedNodeSize = Math.min(sortedNodeSize, size);
    }

    void removeIndex(int idx) {
        if (idx == size - 1) {
            removeLast();
            return;
        }
        nodes.removeAt(idx);
        scores.removeAt(idx);
        if (idx < sortedNodeSize) {
            sortedNodeSize--;
        }
        size--;
    }

    @Override
    public String toString() {
        return "NeighborArray[" + size + "]";
    }

    private int ascSortFindRightMostInsertionPoint(float newScore, int bound) {
        int insertionPoint = Arrays.binarySearch(scores.buffer, 0, bound, newScore);
        if (insertionPoint >= 0) {
            // find the right most position with the same score
            while ((insertionPoint < bound - 1) && (scores.get(insertionPoint + 1) == scores.get(insertionPoint))) {
                insertionPoint++;
            }
            insertionPoint++;
        } else {
            insertionPoint = -insertionPoint - 1;
        }
        return insertionPoint;
    }

    private int descSortFindRightMostInsertionPoint(float newScore, int bound) {
        int start = 0;
        int end = bound - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            if (scores.get(mid) < newScore) end = mid - 1;
            else start = mid + 1;
        }
        return start;
    }

    /**
     * Find first non-diverse neighbour among the list of neighbors starting from the most distant
     * neighbours
     */
    private int findWorstNonDiverse(UpdateableRandomVectorScorer scorer) throws IOException {
        int[] uncheckedIndexes = sort(scorer);
        assert uncheckedIndexes != null : "We will always have something unchecked";
        int uncheckedCursor = uncheckedIndexes.length - 1;
        for (int i = size - 1; i > 0; i--) {
            if (uncheckedCursor < 0) {
                // no unchecked node left
                break;
            }
            scorer.setScoringOrdinal(nodes.get(i));
            if (isWorstNonDiverse(i, uncheckedIndexes, uncheckedCursor, scorer)) {
                return i;
            }
            if (i == uncheckedIndexes[uncheckedCursor]) {
                uncheckedCursor--;
            }
        }
        return size - 1;
    }

    private boolean isWorstNonDiverse(int candidateIndex, int[] uncheckedIndexes, int uncheckedCursor, RandomVectorScorer scorer)
        throws IOException {
        float minAcceptedSimilarity = scores.get(candidateIndex);
        if (candidateIndex == uncheckedIndexes[uncheckedCursor]) {
            // the candidate itself is unchecked
            for (int i = candidateIndex - 1; i >= 0; i--) {
                float neighborSimilarity = scorer.score(nodes.get(i));
                // candidate node is too similar to node i given its score relative to the base node
                if (neighborSimilarity >= minAcceptedSimilarity) {
                    return true;
                }
            }
        } else {
            // else we just need to make sure candidate does not violate diversity with the (newly
            // inserted) unchecked nodes
            assert candidateIndex > uncheckedIndexes[uncheckedCursor];
            for (int i = uncheckedCursor; i >= 0; i--) {
                float neighborSimilarity = scorer.score(nodes.get(uncheckedIndexes[i]));
                // candidate node is too similar to node i given its score relative to the base node
                if (neighborSimilarity >= minAcceptedSimilarity) {
                    return true;
                }
            }
        }
        return false;
    }

    public int maxSize() {
        return maxSize;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + nodes.ramBytesUsed() + scores.ramBytesUsed();
    }
}
