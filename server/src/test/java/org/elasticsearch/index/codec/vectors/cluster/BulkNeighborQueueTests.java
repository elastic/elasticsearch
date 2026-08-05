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
        assertEquals(1, queue.insertWithOverflowBulk(new int[] { 5 }, new float[] { 1.0f }, 1, 1.0f));
        assertEquals(1, queue.insertWithOverflowBulk(new int[] { 4 }, new float[] { 1.0f }, 1, 1.0f));

        List<Long> drained = drainEncoded(queue);
        assertEquals(1, drained.size());
        long encoded = drained.get(0);
        assertEquals(4, queue.decodeNodeId(encoded));
        assertEquals(1.0f, queue.decodeScore(encoded), 0.0f);
    }

    private static void assertTopKMatches(BulkNeighborQueue queue, int[] docs, float[] scores, int count, int k, float bestScore) {
        queue.insertWithOverflowBulk(docs, scores, count, bestScore);
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
