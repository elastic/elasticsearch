/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AsyncDirectIOIndexInputTests extends ESTestCase {

    @SuppressForbidden(reason = "requires Files.getFileStore")
    private static int getBlockSize(Path path) throws IOException {
        return Math.toIntExact(Files.getFileStore(path).getBlockSize());
    }

    private static final int BASE_BUFFER_SIZE = 8192;

    public void testPrefetchEdgeCase() throws IOException {
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 32 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        int offset = 84;
        float[] vectorActual = new float[768];
        int[] toSeek = new int[] { 1, 2, 3, 5, 6, 9, 11, 14, 15, 16, 18, 23, 24, 25, 26, 29, 30, 31 };
        int byteSize = vectorActual.length * Float.BYTES;
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        random().nextBytes(bytes);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (
                AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(
                    path.resolve("test"),
                    blockSize,
                    BASE_BUFFER_SIZE,
                    toSeek.length + 1
                );
            ) {
                IndexInput actualSlice = actualInput.slice("vectors", offset, bytes.length - offset);
                for (int seek : toSeek) {
                    actualSlice.prefetch((long) seek * byteSize, byteSize);
                }
                for (int seek : toSeek) {
                    actualSlice.seek((long) seek * byteSize);
                    actualSlice.readFloats(vectorActual, 0, vectorActual.length);
                    assertEquals("mismatch at seek: " + seek, (seek + 1) * byteSize, actualSlice.getFilePointer());
                }
            }
        }
    }

    public void testLargePrefetch() throws IOException {
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 10 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        int offset = randomIntBetween(1, BASE_BUFFER_SIZE);
        int numBytes = randomIntBetween(BASE_BUFFER_SIZE + 1, BASE_BUFFER_SIZE * 8);
        random().nextBytes(bytes);
        byte[] trueBytes = new byte[numBytes];
        System.arraycopy(bytes, offset, trueBytes, 0, numBytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (
                AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(
                    path.resolve("test"),
                    blockSize,
                    blockSize,
                    randomIntBetween(2, 16)
                );
            ) {
                byte[] actualBytes = new byte[numBytes];
                // prefetch everything at once
                actualInput.prefetch(offset, numBytes);
                actualInput.seek(offset);
                actualInput.readBytes(actualBytes, 0, actualBytes.length);
                assertArrayEquals(trueBytes, actualBytes);
            }
        }
    }

    public void testWriteThenReadBytesConsistency() throws IOException {
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 8 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        int bufferSize = 1024 * 4;
        List<Integer> seeks = new ArrayList<>();
        int lastSeek = 0;
        seeks.add(0);
        while (lastSeek < bytes.length) {
            int nextSeek = randomIntBetween(lastSeek, Math.min(lastSeek + bufferSize, bytes.length - 1));
            seeks.add(nextSeek);
            lastSeek = nextSeek + 1;
        }
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (
                AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(
                    path.resolve("test"),
                    blockSize,
                    bufferSize,
                    seeks.size()
                );
                IndexInput expectedInput = dir.openInput("test", org.apache.lucene.store.IOContext.DEFAULT)
            ) {
                assert expectedInput instanceof AsyncDirectIOIndexInput == false;
                byte[] actualBytes = new byte[bufferSize / 2];
                byte[] expectedBytes = new byte[bufferSize / 2];
                int prevSeek = 0;
                for (int j = 1; j < seeks.size(); j++) {
                    actualInput.seek(prevSeek);
                    expectedInput.seek(prevSeek);
                    int seek = seeks.get(j);
                    int toRead = Math.min(actualBytes.length, bytes.length - prevSeek);
                    expectedInput.readBytes(expectedBytes, 0, toRead);
                    actualInput.readBytes(actualBytes, 0, toRead);
                    prevSeek = seek;
                    assertArrayEquals(expectedBytes, actualBytes);
                }
            }

            try (
                AsyncDirectIOIndexInput actualPretchingInput = new AsyncDirectIOIndexInput(
                    path.resolve("test"),
                    blockSize,
                    bufferSize,
                    seeks.size()
                );
                IndexInput expectedInput = dir.openInput("test", org.apache.lucene.store.IOContext.DEFAULT)
            ) {
                assert expectedInput instanceof AsyncDirectIOIndexInput == false;
                byte[] actualBytes = new byte[bufferSize / 2];
                byte[] expectedBytes = new byte[bufferSize / 2];
                for (int seek : seeks) {
                    // always prefetch just a page
                    actualPretchingInput.prefetch(seek, 1);
                }
                int prevSeek = 0;
                for (int j = 1; j < seeks.size(); j++) {
                    actualPretchingInput.seek(prevSeek);
                    expectedInput.seek(prevSeek);
                    int seek = seeks.get(j);
                    int toRead = Math.min(actualBytes.length, bytes.length - prevSeek);
                    actualPretchingInput.readBytes(actualBytes, 0, toRead);
                    expectedInput.readBytes(expectedBytes, 0, toRead);
                    prevSeek = seek;
                    assertArrayEquals(expectedBytes, actualBytes);
                }
            }
        }
    }

    public void testPrefetchGetsCleanUp() throws IOException {
        int numVectors = randomIntBetween(100, 1000);
        int numDimensions = randomIntBetween(100, 2048);
        Path path = createTempDir("testDirectIODirectory");
        byte[] bytes = new byte[numDimensions * Float.BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        float[][] vectors = new float[numVectors][numDimensions];
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    random().nextBytes(bytes);
                    output.writeBytes(bytes, bytes.length);
                    buffer.asFloatBuffer().get(vectors[i]);
                }
            }

            final int blockSize = getBlockSize(path);
            final int bufferSize = 8192;
            // fetch all
            try (AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, bufferSize, 64)) {
                assertPrefetchSlots(actualInput, numDimensions, numVectors, i -> i, vectors, bufferSize);
            }
            // fetch all in slice
            try (AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, bufferSize, 64)) {
                int start = randomIntBetween(0, numVectors - 1);
                float[][] vectorsSlice = Arrays.copyOfRange(vectors, start, numVectors);
                long sliceStart = (long) start * bytes.length;
                assertPrefetchSlots(
                    (AsyncDirectIOIndexInput) actualInput.slice("slice", sliceStart, actualInput.length() - sliceStart),
                    numDimensions,
                    vectorsSlice.length,
                    i -> i,
                    vectorsSlice,
                    bufferSize
                );
            }
            // random fetch
            List<Integer> tempList = new ArrayList<>(numVectors);
            for (int i = 0; i < numVectors; i++) {
                tempList.add(i);
            }
            Collections.shuffle(tempList, random());
            List<Integer> subList = tempList.subList(0, randomIntBetween(1, numVectors));
            Collections.sort(subList);
            try (AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, bufferSize, 64)) {
                assertPrefetchSlots(actualInput, numDimensions, subList.size(), subList::get, vectors, bufferSize);
            }
        }
    }

    private static void assertPrefetchSlots(
        AsyncDirectIOIndexInput actualInput,
        int numDimensions,
        int numVectors,
        IntToIntFunction ords,
        float[][] vectors,
        int bufferSize
    ) throws IOException {
        int prefetchSize = randomIntBetween(1, 64);
        float[] floats = new float[numDimensions];
        long bytesLength = (long) numDimensions * Float.BYTES;
        int limit = numVectors - prefetchSize + 1;
        int i = 0;
        for (; i < limit; i += prefetchSize) {
            int ord = ords.apply(i);
            for (int j = 0; j < prefetchSize; j++) {
                actualInput.prefetch((ord + j) * bytesLength, bytesLength);
            }
            // check we prefetch enough data. We need to add 1 because of the current buffer.
            assertThat(prefetchSize * bytesLength, lessThanOrEqualTo((long) (1 + actualInput.prefetchSlots()) * bufferSize));
            for (int j = 0; j < prefetchSize; j++) {
                actualInput.seek((ord + j) * bytesLength);
                actualInput.readFloats(floats, 0, floats.length);
                assertArrayEquals(vectors[ord + j], floats, 0.0f);
            }
            // check we have freed all the slots
            assertEquals(0, actualInput.prefetchSlots());
        }
        for (int k = i; k < numVectors; k++) {
            actualInput.prefetch(ords.apply(k) * bytesLength, bytesLength);
        }
        // check we prefetch enough data. We need to add 1 because of the current buffer.
        assertThat((numVectors - i) * bytesLength, lessThanOrEqualTo((long) (1 + actualInput.prefetchSlots()) * bufferSize));
        for (; i < numVectors; i++) {
            int ord = ords.apply(i);
            actualInput.seek(ord * bytesLength);
            actualInput.readFloats(floats, 0, floats.length);
            assertArrayEquals(vectors[ord], floats, 0.0f);
        }
        // check we have freed all the slots
        assertEquals(0, actualInput.prefetchSlots());
    }
}
