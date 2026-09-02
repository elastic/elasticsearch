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
import org.apache.lucene.util.ThreadInterruptedException;
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

    /**
     * Closing a slice with prefetches in flight must not close the {@link java.nio.channels.FileChannel} it shares
     * with its parent (see #158421).
     */
    public void testCloseSliceDoesNotCloseSharedChannel() throws IOException {
        doTestCloseDoesNotCloseSharedChannel(false);
    }

    /**
     * Same as {@link #testCloseSliceDoesNotCloseSharedChannel} for clones.
     */
    public void testCloseCloneDoesNotCloseSharedChannel() throws IOException {
        doTestCloseDoesNotCloseSharedChannel(true);
    }

    private void doTestCloseDoesNotCloseSharedChannel(boolean useClone) throws IOException {
        Path path = createTempDir("testCloseDoesNotCloseSharedChannel");
        int blockSize = getBlockSize(path);
        // large buffers keep the prefetch reads in flight long enough for close() to race them
        final int bufferSize = (1 << 20) / blockSize * blockSize;
        final int maxPrefetches = 4;
        final int numBuffers = 8;
        final int fileSize = bufferSize * numBuffers;
        final byte[] fileData = new byte[fileSize];
        random().nextBytes(fileData);

        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(fileData, fileData.length);
            }

            try (AsyncDirectIOIndexInput parent = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, bufferSize, maxPrefetches)) {
                for (int iter = 0; iter < 10; iter++) {
                    IndexInput child = useClone ? parent.clone() : parent.slice("child", 0, parent.length());
                    // the first buffer is already loaded, so prefetch the ones after it
                    for (int i = 1; i < maxPrefetches; i++) {
                        child.prefetch((long) bufferSize * i, bufferSize);
                    }
                    child.close();
                    child.close(); // closing twice is a no-op

                    final int readLen = blockSize;
                    parent.seek(0L);
                    byte[] actual1 = new byte[readLen];
                    parent.readBytes(actual1, 0, readLen);
                    assertArrayEquals("parent read at offset 0 failed on iter " + iter, Arrays.copyOfRange(fileData, 0, readLen), actual1);

                    long pos2 = (long) bufferSize * (numBuffers - 1);
                    parent.seek(pos2);
                    byte[] actual2 = new byte[readLen];
                    parent.readBytes(actual2, 0, readLen);
                    assertArrayEquals(
                        "parent read at offset " + pos2 + " failed on iter " + iter,
                        Arrays.copyOfRange(fileData, (int) pos2, (int) pos2 + readLen),
                        actual2
                    );
                }

                try (IndexInput freshClone = parent.clone()) {
                    freshClone.seek(0L);
                    byte[] cloneBytes = new byte[blockSize];
                    freshClone.readBytes(cloneBytes, 0, cloneBytes.length);
                    assertArrayEquals(Arrays.copyOfRange(fileData, 0, blockSize), cloneBytes);
                }
            }
        }
    }

    /**
     * An interrupt while waiting on a prefetch must surface as {@link ThreadInterruptedException} rather than falling back to
     * a synchronous read on the interrupted thread, which would close the shared channel (see #158421). Whether the interrupt
     * hits depends on whether the prefetch has already completed, so both outcomes are accepted.
     */
    public void testInterruptWhileWaitingForPrefetchDoesNotCloseChannel() throws IOException {
        Path path = createTempDir("testInterruptDoesNotCloseChannel");
        int blockSize = getBlockSize(path);
        final int bufferSize = (1 << 20) / blockSize * blockSize;
        final int maxPrefetches = 4;
        final int numBuffers = 8;
        final int fileSize = bufferSize * numBuffers;
        final byte[] fileData = new byte[fileSize];
        random().nextBytes(fileData);

        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(fileData, fileData.length);
            }

            try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, bufferSize, maxPrefetches)) {
                final long prefetchPos = bufferSize; // the first buffer is already loaded
                input.prefetch(prefetchPos, bufferSize);

                Thread.currentThread().interrupt();
                try {
                    input.seek(prefetchPos);
                    byte[] actual = new byte[blockSize];
                    input.readBytes(actual, 0, actual.length);
                    // the prefetch completed before the read, so the interrupt was never observed
                    assertArrayEquals(Arrays.copyOfRange(fileData, (int) prefetchPos, (int) prefetchPos + blockSize), actual);
                } catch (ThreadInterruptedException expected) {
                    // the prefetch was still in flight; its slot stays mapped and is consumed by the read below
                } finally {
                    // either way the flag must still be set; Thread.interrupted() also clears it
                    assertTrue("interrupt flag must be set after seek/read on interrupted thread", Thread.interrupted());
                }

                input.seek(prefetchPos);
                byte[] actual2 = new byte[blockSize];
                input.readBytes(actual2, 0, actual2.length);
                assertArrayEquals(
                    "read after clearing interrupt returned wrong bytes",
                    Arrays.copyOfRange(fileData, (int) prefetchPos, (int) prefetchPos + blockSize),
                    actual2
                );
                assertEquals(0, input.prefetchSlots());

                try (IndexInput freshClone = input.clone()) {
                    freshClone.seek(prefetchPos);
                    byte[] cloneBytes = new byte[blockSize];
                    freshClone.readBytes(cloneBytes, 0, cloneBytes.length);
                    assertArrayEquals(Arrays.copyOfRange(fileData, (int) prefetchPos, (int) prefetchPos + blockSize), cloneBytes);
                }
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
