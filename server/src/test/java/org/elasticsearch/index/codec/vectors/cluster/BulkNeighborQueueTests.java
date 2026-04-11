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
        BulkNeighborQueue queue = new BulkNeighborQueue(2, false, BulkNeighborQueue.Strategy.BINARY);
        long sentinel = queue.peek();
        assertEquals(Float.NEGATIVE_INFINITY, queue.decodeScore(sentinel), 0.0f);
        assertEquals(0, queue.size());
    }

    public void testPeekSentinelMaxHeap() {
        BulkNeighborQueue queue = new BulkNeighborQueue(2, true, BulkNeighborQueue.Strategy.BINARY);
        long sentinel = queue.peek();
        assertEquals(Float.POSITIVE_INFINITY, queue.decodeScore(sentinel), 0.0f);
        assertEquals(0, queue.size());
    }

    public void testBulkInsertMatchesTopKMinHeap() {
        int[] docs = new int[] { 1, 2, 3, 4, 5, 6 };
        float[] scores = new float[] { 1.0f, 0.5f, 2.0f, 1.5f, 2.0f, 0.1f };
        int count = docs.length;
        int k = 3;

        assertTopKMatches(new BulkNeighborQueue(k, false, BulkNeighborQueue.Strategy.BINARY), docs, scores, count, k, maxScore(scores));
        assertTopKMatches(
            new BulkNeighborQueue(k, false, BulkNeighborQueue.Strategy.QUICKSELECT),
            docs,
            scores,
            count,
            k,
            maxScore(scores)
        );
        assertTopKMatches(new BulkNeighborQueue(k, false, BulkNeighborQueue.Strategy.FAISS_RESERVOIR), docs, scores, count, k, maxScore(scores));
        assertTopKMatches(new BulkNeighborQueue(k, false, BulkNeighborQueue.Strategy.SCANN_FAST), docs, scores, count, k, maxScore(scores));
        assertTopKMatches(new BulkNeighborQueue(k, false, BulkNeighborQueue.Strategy.AUTO_V2, count * 10), docs, scores, count, k, maxScore(scores));
    }

    public void testBulkInsertMatchesTopKMaxHeap() {
        int[] docs = new int[] { 1, 2, 3, 4, 5, 6 };
        float[] scores = new float[] { 1.0f, 0.5f, 2.0f, 1.5f, 2.0f, 0.1f };
        int count = docs.length;
        int k = 3;

        assertTopKMatches(new BulkNeighborQueue(k, true, BulkNeighborQueue.Strategy.BINARY), docs, scores, count, k, minScore(scores));
    }

    public void testNonBinaryStrategiesRequireMinHeap() {
        expectThrows(IllegalArgumentException.class, () -> new BulkNeighborQueue(2, true, BulkNeighborQueue.Strategy.QUICKSELECT));
        expectThrows(IllegalArgumentException.class, () -> new BulkNeighborQueue(2, true, BulkNeighborQueue.Strategy.FAISS_RESERVOIR));
        expectThrows(IllegalArgumentException.class, () -> new BulkNeighborQueue(2, true, BulkNeighborQueue.Strategy.SCANN_FAST));
        expectThrows(IllegalArgumentException.class, () -> new BulkNeighborQueue(2, true, BulkNeighborQueue.Strategy.AUTO_V2));
    }

    public void testAutoV2UsesBinaryWhenFewVectorsAreExpected() {
        int[] docs = new int[] { 1, 2, 3, 4 };
        float[] scores = new float[] { 1.0f, 0.5f, 2.0f, 1.5f };
        int k = 4;
        BulkNeighborQueue queue = new BulkNeighborQueue(k, false, BulkNeighborQueue.Strategy.AUTO_V2, k);
        assertTopKMatches(queue, docs, scores, docs.length, k, maxScore(scores));
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

    private static float minScore(float[] scores) {
        float min = Float.POSITIVE_INFINITY;
        for (float score : scores) {
            min = Math.min(min, score);
        }
        return min;
    }
}
