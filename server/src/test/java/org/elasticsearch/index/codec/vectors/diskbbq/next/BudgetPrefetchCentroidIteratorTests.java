/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;
import org.elasticsearch.index.codec.vectors.diskbbq.PrefetchingCentroidIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BudgetPrefetchCentroidIteratorTests extends ESTestCase {

    public void testInitialBatchPrefetchesBeforeRingFill() throws IOException {
        List<PostingMetadata> elements = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            elements.add(new PostingMetadata(100L * i, 50, 0, 0f));
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput base = createDummyIndexInput(dir, 2000L);
            List<Long> prefetchOffsets = new ArrayList<>();
            IndexInput recording = new FilterIndexInput("rec", base) {
                @Override
                public void prefetch(long offset, long length) throws IOException {
                    prefetchOffsets.add(offset);
                    super.prefetch(offset, length);
                }
            };
            int batch = 3;
            int ring = 2;
            new BudgetPrefetchCentroidIterator(new ListCentroidIterator(elements), recording, batch, ring);
            assertEquals(batch + ring, prefetchOffsets.size());
            for (int i = 0; i < batch + ring; i++) {
                assertEquals(100L * i, prefetchOffsets.get(i).longValue());
            }
            recording.close();
        }
    }

    public void testZeroInitialBatchMatchesPrefetchingCentroidIteratorDepth() throws IOException {
        int numElements = randomIntBetween(5, 40);
        List<PostingMetadata> elements = generatePostingMetadata(numElements);
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, numElements * 100L);
            int depth = randomIntBetween(1, Math.min(8, numElements));
            List<PostingMetadata> baseline = drainPrefetching(new ListCentroidIterator(elements), indexInput, depth);
            List<PostingMetadata> budget = drainBudget(new ListCentroidIterator(elements), indexInput, 0, depth);
            assertEquals(baseline, budget);
            indexInput.close();
        }
    }

    public void testSameSequenceForDifferentRingDepthsWithFixedBatch() throws IOException {
        int numElements = randomIntBetween(10, 50);
        List<PostingMetadata> elements = generatePostingMetadata(numElements);
        int batch = randomIntBetween(1, Math.min(5, numElements));
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, ElementsTotalLength(elements));
            List<PostingMetadata> baseline = drainBudget(new ListCentroidIterator(elements), indexInput, batch, 1);
            indexInput.close();
            for (int ring : new int[] { 2, 4, 8 }) {
                if (ring > numElements) {
                    continue;
                }
                IndexInput in2 = createDummyIndexInput(dir, ElementsTotalLength(elements));
                assertEquals(baseline, drainBudget(new ListCentroidIterator(elements), in2, batch, ring));
                in2.close();
            }
        }
    }

    public void testIllegalRingDepth() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, 100L);
            List<PostingMetadata> elements = List.of(new PostingMetadata(0, 10, 0, 0f));
            expectThrows(
                IllegalArgumentException.class,
                () -> new BudgetPrefetchCentroidIterator(new ListCentroidIterator(elements), indexInput, 0, 0)
            );
            indexInput.close();
        }
    }

    public void testComputeInitialCentroidBatchSizeRespectsFilter() {
        assertEquals(3, ESNextDiskBBQVectorsReader.computeInitialCentroidBatchSize(0.3f, 10, null));
        FixedBitSet bits = new FixedBitSet(10);
        bits.set(1);
        bits.set(3);
        assertEquals(2, ESNextDiskBBQVectorsReader.computeInitialCentroidBatchSize(1f, 10, bits));
        FixedBitSet empty = new FixedBitSet(10);
        assertEquals(0, ESNextDiskBBQVectorsReader.computeInitialCentroidBatchSize(0.5f, 10, empty));
    }

    public void testIllegalNegativeBatch() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, 100L);
            List<PostingMetadata> elements = List.of(new PostingMetadata(0, 10, 0, 0f));
            expectThrows(
                IllegalArgumentException.class,
                () -> new BudgetPrefetchCentroidIterator(new ListCentroidIterator(elements), indexInput, -1, 1)
            );
            indexInput.close();
        }
    }

    public void testEmptyDelegate() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, 10L);
            BudgetPrefetchCentroidIterator iter = new BudgetPrefetchCentroidIterator(new ListCentroidIterator(List.of()), indexInput, 0, 1);
            assertFalse(iter.hasNext());
            indexInput.close();
        }
    }

    public void testEmptyDelegateWithPositiveBatch() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, 10L);
            BudgetPrefetchCentroidIterator iter = new BudgetPrefetchCentroidIterator(new ListCentroidIterator(List.of()), indexInput, 5, 2);
            assertFalse(iter.hasNext());
            indexInput.close();
        }
    }

    public void testInitialBatchLargerThanDelegateConsumesAllInBatch() throws IOException {
        List<PostingMetadata> elements = List.of(new PostingMetadata(10, 20, 0, 0.1f), new PostingMetadata(30, 40, 1, 0.2f));
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput base = createDummyIndexInput(dir, 500L);
            List<long[]> prefetches = new ArrayList<>();
            IndexInput recording = new FilterIndexInput("rec", base) {
                @Override
                public void prefetch(long offset, long length) throws IOException {
                    prefetches.add(new long[] { offset, length });
                    super.prefetch(offset, length);
                }
            };
            BudgetPrefetchCentroidIterator iter = new BudgetPrefetchCentroidIterator(new ListCentroidIterator(elements), recording, 100, 3);
            assertEquals(2, prefetches.size());
            assertArrayEquals(new long[] { 10, 20 }, prefetches.get(0));
            assertArrayEquals(new long[] { 30, 40 }, prefetches.get(1));
            assertEquals(elements, drain(iter));
            recording.close();
        }
    }

    public void testNextThrowsWhenDrained() throws IOException {
        List<PostingMetadata> elements = List.of(new PostingMetadata(0, 10, 0, 0f));
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, 100L);
            BudgetPrefetchCentroidIterator iter = new BudgetPrefetchCentroidIterator(new ListCentroidIterator(elements), indexInput, 0, 1);
            assertNotNull(iter.nextPosting());
            assertFalse(iter.hasNext());
            expectThrows(IllegalStateException.class, iter::nextPosting);
            indexInput.close();
        }
    }

    public void testFullPrefetchTraceMatchesDrainOrder() throws IOException {
        int n = 10;
        List<PostingMetadata> elements = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            elements.add(new PostingMetadata(1000L * i, 10 + i, i, 0.01f * i));
        }
        int batch = 3;
        int ring = 2;
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput base = createDummyIndexInput(dir, 50_000L);
            List<long[]> prefetches = new ArrayList<>();
            IndexInput recording = new FilterIndexInput("rec", base) {
                @Override
                public void prefetch(long offset, long length) throws IOException {
                    prefetches.add(new long[] { offset, length });
                    super.prefetch(offset, length);
                }
            };
            BudgetPrefetchCentroidIterator iter = new BudgetPrefetchCentroidIterator(
                new ListCentroidIterator(elements),
                recording,
                batch,
                ring
            );
            for (int i = 0; i < n; i++) {
                PostingMetadata md = iter.nextPosting();
                assertEquals(elements.get(i), md);
            }
            assertFalse(iter.hasNext());

            assertEquals(n, prefetches.size());
            for (int i = 0; i < n; i++) {
                PostingMetadata expected = elements.get(i);
                long[] want = new long[] { expected.offset(), expected.length() };
                assertArrayEquals("prefetch index " + i, want, prefetches.get(i));
            }
            recording.close();
        }
    }

    public void testPreservesPostingMetadataFields() throws IOException {
        List<PostingMetadata> elements = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            elements.add(new PostingMetadata(100L * i, 50 + i, i % 5, 0.1f * i));
        }
        int batch = 4;
        int ring = 3;
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, ElementsTotalLength(elements));
            List<PostingMetadata> out = drainBudget(new ListCentroidIterator(elements), indexInput, batch, ring);
            assertEquals(elements.size(), out.size());
            for (int i = 0; i < elements.size(); i++) {
                PostingMetadata e = elements.get(i);
                PostingMetadata a = out.get(i);
                assertEquals(e.offset(), a.offset());
                assertEquals(e.length(), a.length());
                assertEquals(e.queryCentroidOrdinal(), a.queryCentroidOrdinal());
                assertEquals(e.documentCentroidScore(), a.documentCentroidScore(), 0f);
            }
            indexInput.close();
        }
    }

    public void testRingDepthOneSingleElement() throws IOException {
        List<PostingMetadata> elements = List.of(new PostingMetadata(7, 11, 42, 3.14f));
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, 100L);
            BudgetPrefetchCentroidIterator iter = new BudgetPrefetchCentroidIterator(new ListCentroidIterator(elements), indexInput, 1, 1);
            assertTrue(iter.hasNext());
            PostingMetadata md = iter.nextPosting();
            assertEquals(7, md.offset());
            assertEquals(42, md.queryCentroidOrdinal());
            assertFalse(iter.hasNext());
            indexInput.close();
        }
    }

    private static long ElementsTotalLength(List<PostingMetadata> elements) {
        long t = 0;
        for (PostingMetadata p : elements) {
            t += p.length();
        }
        return Math.max(t, 100L);
    }

    private List<PostingMetadata> generatePostingMetadata(int count) {
        List<PostingMetadata> elements = new ArrayList<>();
        long offset = 0;
        for (int i = 0; i < count; i++) {
            long length = randomIntBetween(10, 200);
            elements.add(new PostingMetadata(offset, length, i, randomFloat()));
            offset += length;
        }
        return elements;
    }

    private List<PostingMetadata> drainPrefetching(CentroidIterator delegate, IndexInput indexInput, int depth) throws IOException {
        PrefetchingCentroidIterator iter = new PrefetchingCentroidIterator(delegate, indexInput, depth);
        return drain(iter);
    }

    private List<PostingMetadata> drainBudget(CentroidIterator delegate, IndexInput indexInput, int batch, int ring) throws IOException {
        BudgetPrefetchCentroidIterator iter = new BudgetPrefetchCentroidIterator(delegate, indexInput, batch, ring);
        return drain(iter);
    }

    private List<PostingMetadata> drain(CentroidIterator iter) throws IOException {
        List<PostingMetadata> result = new ArrayList<>();
        while (iter.hasNext()) {
            result.add(iter.nextPosting());
        }
        return result;
    }

    private IndexInput createDummyIndexInput(Directory dir, long size) throws IOException {
        String name = "dummy_" + randomAlphaOfLength(6);
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            byte[] zeros = new byte[(int) Math.min(size, 4096)];
            long remaining = size;
            while (remaining > 0) {
                int toWrite = (int) Math.min(remaining, zeros.length);
                out.writeBytes(zeros, toWrite);
                remaining -= toWrite;
            }
        }
        return dir.openInput(name, IOContext.DEFAULT);
    }

    static final class ListCentroidIterator implements CentroidIterator {
        private final List<PostingMetadata> elements;
        private int index = 0;

        ListCentroidIterator(List<PostingMetadata> elements) {
            this.elements = new ArrayList<>(elements);
        }

        @Override
        public boolean hasNext() {
            return index < elements.size();
        }

        @Override
        public PostingMetadata nextPosting() {
            return elements.get(index++);
        }
    }
}
