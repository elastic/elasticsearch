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
import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AsyncDirectIOIndexInputTests extends ESTestCase {

    @SuppressForbidden(reason = "requires Files.getFileStore")
    private static int getBlockSize(Path path) throws IOException {
        return Math.toIntExact(Files.getFileStore(path).getBlockSize());
    }

    private byte[] writeTestFile(Path dir) throws IOException {
        byte[] data = new byte[BASE_BUFFER_SIZE * 4];
        random().nextBytes(data);
        try (Directory d = new NIOFSDirectory(dir)) {
            try (var out = d.createOutput("test", org.apache.lucene.store.IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }
        }
        return data;
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
     * Closing a slice with a prefetch in flight must not close the {@link java.nio.channels.FileChannel} it shares
     * with its parent.
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

    @SuppressForbidden(reason = "requires FileChannel#open and FilterFileChannel#read for deterministic race injection")
    private void doTestCloseDoesNotCloseSharedChannel(boolean useClone) throws IOException {
        Path path = createTempDir("testCloseDoesNotCloseSharedChannel");
        int blockSize = getBlockSize(path);
        byte[] fileData = writeTestFile(path);

        // Latches make the race deterministic: the prefetch virtual thread parks inside FileChannel.read until
        // the test has called child.close(), ensuring shutdownNow() (pre-fix) always races an in-flight read.
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch allowRead = new CountDownLatch(1);
        CountDownLatch readDone = new CountDownLatch(1);
        AtomicBoolean blockReads = new AtomicBoolean(false);

        try (FileChannel realChannel = FileChannel.open(path.resolve("test"), StandardOpenOption.READ)) {
            var blockingChannel = new FilterFileChannel(realChannel) {
                @Override
                public int read(ByteBuffer dst, long position) throws IOException {
                    if (blockReads.get()) {
                        readStarted.countDown();
                        try {
                            allowRead.await();
                        } catch (InterruptedException e) {
                            // Restore the flag so the subsequent FileChannel.read sees it and throws ClosedByInterruptException,
                            // closing the channel — that is precisely the pre-fix failure mode we are testing against.
                            Thread.currentThread().interrupt();
                        }
                    }
                    try {
                        return super.read(dst, position);
                    } finally {
                        if (blockReads.get()) {
                            readDone.countDown();
                        }
                    }
                }
            };

            try (AsyncDirectIOIndexInput parent = new AsyncDirectIOIndexInput(blockingChannel, blockSize, BASE_BUFFER_SIZE, 2)) {
                IndexInput child = useClone ? parent.clone() : parent.slice("child", 0, parent.length());

                blockReads.set(true);
                child.prefetch(BASE_BUFFER_SIZE, BASE_BUFFER_SIZE); // submits a virtual thread that will park in read()
                try {
                    assertTrue("prefetch read did not start in time", readStarted.await(10, TimeUnit.SECONDS));
                    child.close(); // pre-fix: shutdownNow() interrupts the parked thread; post-fix: shutdown() does not
                    allowRead.countDown(); // post-fix: let the thread proceed; pre-fix: thread already exited via interrupt
                    assertTrue("prefetch read did not finish in time", readDone.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }

                // ClosedChannelException here means close() indirectly closed the shared channel
                parent.seek(BASE_BUFFER_SIZE);
                assertEquals(fileData[BASE_BUFFER_SIZE], parent.readByte());
            }
        }
    }

    /**
     * An interrupt while waiting on a prefetch must surface as {@link ThreadInterruptedException} and must not close the
     * shared {@link java.nio.channels.FileChannel} (see #158421). The blocking channel guarantees the prefetch is still
     * in flight when the interrupt fires, so the {@link InterruptedException} branch is always exercised.
     */
    @SuppressForbidden(reason = "requires FileChannel#open and FilterFileChannel#read for deterministic race injection")
    public void testInterruptWhileWaitingForPrefetchDoesNotCloseChannel() throws IOException {
        Path path = createTempDir("testInterruptDoesNotCloseChannel");
        int blockSize = getBlockSize(path);
        byte[] fileData = writeTestFile(path);

        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch allowRead = new CountDownLatch(1);
        AtomicBoolean blockReads = new AtomicBoolean(false);

        try (FileChannel realChannel = FileChannel.open(path.resolve("test"), StandardOpenOption.READ)) {
            var blockingChannel = new FilterFileChannel(realChannel) {
                @Override
                public int read(ByteBuffer dst, long position) throws IOException {
                    if (blockReads.get()) {
                        readStarted.countDown();
                        try {
                            allowRead.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return super.read(dst, position);
                }
            };

            try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(blockingChannel, blockSize, BASE_BUFFER_SIZE, 2)) {
                blockReads.set(true);
                input.prefetch(BASE_BUFFER_SIZE, BASE_BUFFER_SIZE); // virtual thread will park inside read()
                try {
                    assertTrue("prefetch read did not start in time", readStarted.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }

                // prefetch is blocked; Future.get() sees the interrupt flag and throws ThreadInterruptedException
                Thread.currentThread().interrupt();
                try {
                    expectThrows(ThreadInterruptedException.class, () -> {
                        input.seek(BASE_BUFFER_SIZE);
                        input.readByte();
                    });
                } finally {
                    assertTrue(Thread.interrupted()); // flag must be set; also clears it
                    allowRead.countDown(); // release the parked prefetch thread
                }

                // channel must still be open; slot stays mapped and is consumed by this read
                input.seek(BASE_BUFFER_SIZE);
                assertEquals(fileData[BASE_BUFFER_SIZE], input.readByte());
                assertEquals(0, input.prefetchSlots());
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
