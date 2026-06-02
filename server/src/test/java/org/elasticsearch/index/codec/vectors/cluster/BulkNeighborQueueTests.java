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
package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BulkNeighborQueueTests extends ESTestCase {

    public void testPeekSentinelMinHeap() {
        BulkNeighborQueue queue = new BulkNeighborQueue(2);
        long sentinel = queue.peek();
        assertEquals(0.0f, queue.decodeScore(sentinel), 0.0f);
        assertEquals(0, queue.size());
    }

    public void testMaxSizeMustBePositive() {
        expectThrows(IllegalArgumentException.class, () -> new BulkNeighborQueue(0));
    }

    public void testBulkInsertMatchesTopKMinHeap() {
        int[] docs = new int[] { 1, 2, 3, 4, 5, 6 };
        float[] scores = new float[] { 1.0f, 0.5f, 2.0f, 1.5f, 2.0f, 0.1f };
        int count = docs.length;
        int k = 3;

        assertTopKMatches(new BulkNeighborQueue(k), docs, scores, count, k, maxScore(scores));
    }

    public void testAutoWorksWhenFewVectorsAreExpected() {
        int[] docs = new int[] { 1, 2, 3, 4 };
        float[] scores = new float[] { 1.0f, 0.5f, 2.0f, 1.5f };
        int k = 4;
        BulkNeighborQueue queue = new BulkNeighborQueue(k);
        assertTopKMatches(queue, docs, scores, docs.length, k, maxScore(scores));
    }

    public void testTinyPathBoundaryAtTen() {
        int[] docs = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        float[] scores = new float[] { 1.0f, 0.5f, 2.0f, 1.5f, 3.0f, 2.8f, 2.2f, 0.1f, 1.7f, 4.0f, 3.5f, 3.9f };
        int k = 10;
        assertTopKMatches(new BulkNeighborQueue(k), docs, scores, docs.length, k, maxScore(scores));
    }

    public void testReservoirPathAboveTinyBoundary() {
        int[] docs = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        float[] scores = new float[] { 1.0f, 0.5f, 2.0f, 1.5f, 3.0f, 2.8f, 2.2f, 0.1f, 1.7f, 4.0f, 3.5f, 3.9f };
        int k = 11;
        assertTopKMatches(new BulkNeighborQueue(k), docs, scores, docs.length, k, maxScore(scores));
    }

    public void testEqualBestScoreStillUsesDocTieBreak() {
        BulkNeighborQueue queue = new BulkNeighborQueue(1);
        long noFilter = NeighborQueue.encodeRaw(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);
        assertEquals(1, queue.insertWithOverflowBulk(new int[] { 5 }, new float[] { 1.0f }, 1, 1.0f, noFilter));
        assertEquals(1, queue.insertWithOverflowBulk(new int[] { 4 }, new float[] { 1.0f }, 1, 1.0f, noFilter));

        List<Long> drained = drainEncoded(queue);
        assertEquals(1, drained.size());
        long encoded = drained.get(0);
        assertEquals(4, queue.decodeNodeId(encoded));
        assertEquals(1.0f, queue.decodeScore(encoded), 0.0f);
    }

    public void testMinCompetitiveDocScoreAtLongMinValueHandledCorrectly() {
        BulkNeighborQueue queue = BulkNeighborQueue.forMerging(1);
        queue.insertWithOverflowBulk(new int[] { 5 }, new float[] { 0.0f }, 1, 0.0f, Long.MIN_VALUE);
        queue.insertWithOverflowBulk(new int[] { 6 }, new float[] { 1.0f }, 1, 1.0f, Long.MIN_VALUE);

        List<Long> drained = drainEncoded(queue);
        assertEquals(1, drained.size());
        assertEquals(6, queue.decodeNodeId(drained.getFirst()));
        assertEquals(1.0f, queue.decodeScore(drained.getFirst()), 0.0f);
    }

    public void testMinCompetitiveDocScoreFiltersInReservoirPath() {
        // k > TINY_K_BINARY_THRESHOLD (10) forces the reservoir path where minCompetitiveDocScore is applied
        int k = 11;
        BulkNeighborQueue queue = new BulkNeighborQueue(k);

        // Fill the queue with scores 1.0 to 2.0 so the internal threshold is encodeRaw(0, 1.0f)
        int[] initialDocs = new int[k];
        float[] initialScores = new float[k];
        for (int i = 0; i < k; i++) {
            initialDocs[i] = i;
            initialScores[i] = 1.0f + i * 0.1f;
        }
        long noFilter = NeighborQueue.encodeRaw(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);
        queue.insertWithOverflowBulk(initialDocs, initialScores, k, 2.0f, noFilter);

        // minCompetitiveDocScore = encodeRaw(MAX, 1.5f) raises the effective threshold above the
        // queue's own threshold of 1.0f: only candidates with score > 1.5f should be accepted
        long minCompScore = NeighborQueue.encodeRaw(Integer.MAX_VALUE, 1.5f);
        int[] newDocs = new int[] { 100, 101, 102, 103 };
        float[] newScores = new float[] { 1.2f, 1.4f, 1.6f, 1.8f };
        int accepted = queue.insertWithOverflowBulk(newDocs, newScores, 4, 1.8f, minCompScore);

        // Docs with scores 1.2 and 1.4 are below the competitive threshold and must be rejected;
        // docs with scores 1.6 and 1.8 exceed it and must be accepted
        assertEquals(2, accepted);
    }

    public void testMergeDoesNotFilterKthDocWhenItEqualsMinCompetitiveDocScore() {
        // The kth doc from segment B has encoded value == minCompetitiveDocScore.
        // It must still appear in the global top-mergeK after merge.
        int mergeK = 4;
        BulkNeighborQueue mergeQueue = BulkNeighborQueue.forMerging(mergeK);

        // Segment A: fill the queue with low-scoring docs (no filtering)
        long noFilter = NeighborQueue.encodeRaw(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);
        mergeQueue.insertWithOverflowBulk(new int[] { 0, 1, 2, 3 }, new float[] { 0f, 1f, 2f, 3f }, 4, 3f, noFilter);

        // Segment B: minCompetitiveDocScore is the exact encoded value of the kth doc (100, 7f)
        long minCompScore = NeighborQueue.encodeRaw(100, 7f);
        mergeQueue.insertWithOverflowBulk(new int[] { 97, 98, 99, 100 }, new float[] { 10f, 9f, 8f, 7f }, 4, 10f, minCompScore);

        // Drain is worst-to-best; global top-4 must be [7, 8, 9, 10], not [3, 8, 9, 10]
        List<Long> drained = drainEncoded(mergeQueue);
        assertEquals(4, drained.size());
        assertEquals(7f, BulkNeighborQueue.decodeScoreRaw(drained.get(0)), 0f);
        assertEquals(8f, BulkNeighborQueue.decodeScoreRaw(drained.get(1)), 0f);
        assertEquals(9f, BulkNeighborQueue.decodeScoreRaw(drained.get(2)), 0f);
        assertEquals(10f, BulkNeighborQueue.decodeScoreRaw(drained.get(3)), 0f);
    }

    private static void assertTopKMatches(BulkNeighborQueue queue, int[] docs, float[] scores, int count, int k, float bestScore) {
        queue.insertWithOverflowBulk(docs, scores, count, bestScore, NeighborQueue.encodeRaw(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY));
        assertEquals(k, queue.size());

        long[] expected = topKEncoded(queue, docs, scores, count, k);
        List<Long> drained = drainEncoded(queue);
        assertEquals(expected.length, drained.size());
        long[] actual = new long[drained.size()];
        for (int i = 0; i < drained.size(); i++) {
            actual[i] = drained.get(i);
        }
        Arrays.sort(expected);
        Arrays.sort(actual);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
        assertEquals(0, queue.size());
    }

    private static long[] topKEncoded(BulkNeighborQueue queue, int[] docs, float[] scores, int count, int k) {
        long[] encoded = new long[count];
        for (int i = 0; i < count; i++) {
            encoded[i] = queue.encode(docs[i], scores[i]);
        }
        Arrays.sort(encoded);
        int size = Math.min(k, count);
        long[] top = new long[size];
        for (int i = 0; i < size; i++) {
            top[i] = encoded[count - 1 - i];
        }
        return top;
    }

    private static List<Long> drainEncoded(BulkNeighborQueue queue) {
        List<Long> drained = new ArrayList<>();
        queue.drain(drained::add);
        return drained;
    }

    private static float maxScore(float[] scores) {
        float max = Float.NEGATIVE_INFINITY;
        for (float score : scores) {
            max = Math.max(max, score);
        }
        return max;
    }

}
