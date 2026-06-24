/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests that {@link PrefetchingCentroidIterator} produces identical iteration
 * sequences regardless of the configured prefetch depth.
 */
public class PrefetchingCentroidIteratorTests extends ESTestCase {

    public void testDifferentDepthsProduceSameSequence() throws IOException {
        int numElements = randomIntBetween(5, 50);
        List<PostingMetadata> elements = generatePostingMetadata(numElements);

        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, numElements * 100L);
            List<PostingMetadata> baseline = drainIterator(new ListCentroidIterator(elements), indexInput, 1);

            for (int depth : new int[] { 2, 4, 8, 16 }) {
                if (depth > numElements) continue;
                List<PostingMetadata> result = drainIterator(new ListCentroidIterator(elements), indexInput, depth);
                assertEquals("depth=" + depth + " should produce the same sequence as depth=1", baseline, result);
            }
            indexInput.close();
        }
    }

    public void testSingleElement() throws IOException {
        List<PostingMetadata> elements = generatePostingMetadata(1);

        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, 100L);
            for (int depth : new int[] { 1, 2, 4 }) {
                List<PostingMetadata> result = drainIterator(new ListCentroidIterator(elements), indexInput, depth);
                assertEquals(1, result.size());
                assertEquals(elements.get(0), result.get(0));
            }
            indexInput.close();
        }
    }

    public void testDepthEqualToElementCount() throws IOException {
        int count = randomIntBetween(2, 10);
        List<PostingMetadata> elements = generatePostingMetadata(count);

        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, count * 100L);
            List<PostingMetadata> result = drainIterator(new ListCentroidIterator(elements), indexInput, count);
            assertEquals(elements, result);
            indexInput.close();
        }
    }

    public void testDepthLargerThanElementCount() throws IOException {
        int count = randomIntBetween(2, 5);
        List<PostingMetadata> elements = generatePostingMetadata(count);

        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, count * 100L);
            List<PostingMetadata> result = drainIterator(new ListCentroidIterator(elements), indexInput, count + 10);
            assertEquals(elements, result);
            indexInput.close();
        }
    }

    public void testIllegalDepthZero() throws IOException {
        List<PostingMetadata> elements = generatePostingMetadata(1);
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, 100L);
            expectThrows(
                IllegalArgumentException.class,
                () -> new PrefetchingCentroidIterator(new ListCentroidIterator(elements), indexInput, 0)
            );
            indexInput.close();
        }
    }

    public void testPreservesPostingMetadataFields() throws IOException {
        List<PostingMetadata> elements = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            elements.add(new PostingMetadata(i * 100L, 50 + i, i % 3, 0.1f * i));
        }

        try (Directory dir = new ByteBuffersDirectory()) {
            IndexInput indexInput = createDummyIndexInput(dir, 1200L);
            for (int depth : new int[] { 1, 3, 5, 10 }) {
                List<PostingMetadata> result = drainIterator(new ListCentroidIterator(elements), indexInput, depth);
                assertEquals(elements.size(), result.size());
                for (int i = 0; i < elements.size(); i++) {
                    PostingMetadata expected = elements.get(i);
                    PostingMetadata actual = result.get(i);
                    assertEquals("offset mismatch at " + i, expected.offset(), actual.offset());
                    assertEquals("length mismatch at " + i, expected.length(), actual.length());
                    assertEquals("ordinal mismatch at " + i, expected.queryCentroidOrdinal(), actual.queryCentroidOrdinal());
                    assertEquals("score mismatch at " + i, expected.documentCentroidScore(), actual.documentCentroidScore(), 0f);
                }
            }
            indexInput.close();
        }
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

    private List<PostingMetadata> drainIterator(CentroidIterator delegate, IndexInput indexInput, int depth) throws IOException {
        PrefetchingCentroidIterator iter = new PrefetchingCentroidIterator(delegate, indexInput, depth);
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

    /**
     * A simple CentroidIterator backed by a list, for testing purposes.
     */
    static class ListCentroidIterator implements CentroidIterator {
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
