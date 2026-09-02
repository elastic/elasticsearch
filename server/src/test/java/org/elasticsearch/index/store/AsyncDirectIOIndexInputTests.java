/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AsyncDirectIOIndexInputTests extends ESTestCase {

    @SuppressForbidden(reason = "requires Files.getFileStore")
    private static int getBlockSize(Path path) throws IOException {
        return Math.toIntExact(Files.getFileStore(path).getBlockSize());
    }

    private static final int BASE_BUFFER_SIZE = 8192;

    /**
     * Slice offsets to exercise: the file start, unaligned offsets inside the first block, the
     * neighbours of a block start and of a buffer-window start, the file's tail, plus random offsets.
     * Offsets stop at {@code fileLength - 2} so every slice is at least two bytes long.
     */
    private static long[] sliceOffsets(int blockSize, int fileLength) {
        long[] fixed = {
            0,
            1,
            7,
            96,
            blockSize - 1,
            blockSize,
            blockSize + 96,
            BASE_BUFFER_SIZE - 1,
            BASE_BUFFER_SIZE,
            BASE_BUFFER_SIZE + 96,
            fileLength - 17 };
        long[] offsets = Arrays.copyOf(fixed, fixed.length + atLeast(3));
        for (int i = fixed.length; i < offsets.length; i++) {
            offsets[i] = randomLongBetween(0, fileLength - 2);
        }
        return offsets;
    }

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
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
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
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
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
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (
                AsyncDirectIOIndexInput actualInput = new AsyncDirectIOIndexInput(
                    path.resolve("test"),
                    blockSize,
                    bufferSize,
                    seeks.size()
                );
                IndexInput expectedInput = dir.openInput("test", IOContext.DEFAULT)
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
                IndexInput expectedInput = dir.openInput("test", IOContext.DEFAULT)
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
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
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

    public void testSliceOffsetMatrix() throws IOException {
        // Slices whose start is not block-aligned must read correct bytes on their first read.
        // This is the matrix that catches a lazily positioned slice resolving to the wrong block
        // (a naive removal of the slice-time seekInternal(0) reads block 0 for every offset).
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 4 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            long[] offsets = sliceOffsets(blockSize, bytes.length);
            try (
                AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, BASE_BUFFER_SIZE, 4);
                IndexInput expectedInput = dir.openInput("test", IOContext.DEFAULT)
            ) {
                for (long offset : offsets) {
                    long len = bytes.length - offset;
                    IndexInput actualSlice = input.slice("test-slice", offset, len);
                    IndexInput expectedSlice = expectedInput.slice("expected-slice", offset, len);
                    // first byte, straight after construction
                    assertEquals("first byte at slice offset " + offset, expectedSlice.readByte(), actualSlice.readByte());
                    // bulk read across at least one refill boundary
                    int toRead = Math.toIntExact(Math.min(len - 1, BASE_BUFFER_SIZE + 3));
                    byte[] actual = new byte[toRead];
                    byte[] expected = new byte[toRead];
                    actualSlice.readBytes(actual, 0, toRead);
                    expectedSlice.readBytes(expected, 0, toRead);
                    assertArrayEquals("bulk read at slice offset " + offset, expected, actual);
                    assertEquals(expectedSlice.getFilePointer(), actualSlice.getFilePointer());
                }
            }
        }
    }

    public void testSliceConstructionDefersFirstRead() throws IOException {
        // slice() must not perform I/O: the first fill is deferred until the first read.
        // (KnnFloatVectorQuery constructs vector values per query only for a null/size check and
        // abandons them; slice construction must therefore be free.)
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 2 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, BASE_BUFFER_SIZE, 4)) {
                long offset = randomIntBetween(0, bytes.length - 2);
                var slice = (AsyncDirectIOIndexInput) input.slice("test-slice", offset, bytes.length - offset);
                assertTrue("slice construction must defer the first fill", slice.isDeferred());
                assertEquals(0L, slice.getFilePointer());
                // a seek to the current position must stay deferred
                slice.seek(0);
                assertTrue("seek(0) on a fresh slice must not trigger a fill", slice.isDeferred());
                assertEquals(0L, slice.getFilePointer());
                // the first read resolves the deferred fill
                assertEquals(bytes[(int) offset], slice.readByte());
                assertFalse(slice.isDeferred());
                assertEquals(1L, slice.getFilePointer());
                // an explicit reposition on a fresh slice cancels the deferred fill
                var slice2 = (AsyncDirectIOIndexInput) input.slice("test-slice-2", offset, bytes.length - offset);
                long target = randomIntBetween(1, Math.toIntExact(slice2.length() - 1));
                slice2.seek(target);
                assertFalse("an explicit seek resolves the deferred fill", slice2.isDeferred());
                assertEquals(bytes[(int) (offset + target)], slice2.readByte());
            }
        }
    }

    public void testCloneParity() throws IOException {
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 4 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, BASE_BUFFER_SIZE, 4)) {
                long offset = randomIntBetween(1, BASE_BUFFER_SIZE);
                long len = bytes.length - offset;
                // clone of a fresh (unfilled) slice starts at the slice's position 0
                IndexInput fresh = input.slice("fresh", offset, len);
                IndexInput freshClone = fresh.clone();
                // clone() is unchanged: it positions eagerly at the parent's file pointer
                assertFalse(((AsyncDirectIOIndexInput) freshClone).isDeferred());
                assertEquals(0L, freshClone.getFilePointer());
                assertEquals(bytes[(int) offset], freshClone.readByte());
                assertEquals(bytes[(int) offset], fresh.readByte());
                // clone after a partial read continues at the parent's position
                IndexInput parent = input.slice("parent", offset, len);
                byte[] skip = new byte[randomIntBetween(1, Math.toIntExact(len - 2))];
                parent.readBytes(skip, 0, skip.length);
                IndexInput clone = parent.clone();
                assertEquals(parent.getFilePointer(), clone.getFilePointer());
                assertEquals(bytes[(int) (offset + skip.length)], clone.readByte());
                assertEquals(bytes[(int) (offset + skip.length)], parent.readByte());
            }
        }
    }

    public void testPrefetchOnFreshSlice() throws IOException {
        // prefetch() issued before the first read of a slice must not corrupt the deferred fill: the
        // first window is read synchronously (the hint for it is absorbed), the windows past it come
        // from the prefetch slots, and the bytes must be right across all of them.
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 8 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, BASE_BUFFER_SIZE, 8)) {
                for (long offset : new long[] { 0, 96, blockSize + 96, BASE_BUFFER_SIZE + randomIntBetween(1, blockSize) }) {
                    long len = bytes.length - offset;
                    IndexInput slice = input.slice("test-slice", offset, len);
                    int toRead = Math.toIntExact(Math.min(len, BASE_BUFFER_SIZE * 2));
                    slice.prefetch(0, toRead);
                    byte[] actual = new byte[toRead];
                    slice.readBytes(actual, 0, toRead);
                    byte[] expected = new byte[toRead];
                    System.arraycopy(bytes, (int) offset, expected, 0, toRead);
                    assertArrayEquals("prefetch-then-read at slice offset " + offset, expected, actual);
                }
                // prefetch a later region, then seek straight to it
                long offset = randomIntBetween(1, BASE_BUFFER_SIZE);
                IndexInput slice = input.slice("test-slice", offset, bytes.length - offset);
                long target = BASE_BUFFER_SIZE * 3 + randomIntBetween(0, blockSize);
                slice.prefetch(target, 256);
                slice.seek(target);
                byte[] actual = new byte[256];
                slice.readBytes(actual, 0, actual.length);
                byte[] expected = new byte[256];
                System.arraycopy(bytes, (int) (offset + target), expected, 0, expected.length);
                assertArrayEquals(expected, actual);
            }
        }
    }

    /**
     * A characterization test, not an endorsement. A seek past a slice's own length that still lands
     * inside the buffer window the eager slice() had filled used to silently reposition, and the read
     * after it served the byte beyond the slice's end. The lazy slice must keep doing exactly that
     * rather than fix the quirk in passing; if the quirk is ever removed on purpose, this test should
     * change with it.
     */
    public void testSeekPastLengthOnFreshSliceMatchesEagerBehavior() throws IOException {
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 4];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, BASE_BUFFER_SIZE, 4)) {
                // an unaligned start inside the first block, so the window the eager slice() filled
                // is [0, BASE_BUFFER_SIZE) of the file
                long offset = randomIntBetween(1, blockSize - 1);
                IndexInput slice = input.slice("short-slice", offset, 4);
                // beyond length 4, but still inside that window
                long target = randomIntBetween(5, Math.toIntExact(BASE_BUFFER_SIZE - offset - 1));
                slice.seek(target);
                assertEquals(target, slice.getFilePointer());
                // the quirk: the read is served from the buffered window
                assertEquals(bytes[(int) (offset + target)], slice.readByte());
                assertEquals(target + 1, slice.getFilePointer());
                // a target beyond that window still throws, as it always did
                IndexInput slice2 = input.slice("short-slice-2", offset, 4);
                long beyond = BASE_BUFFER_SIZE - offset + randomIntBetween(0, blockSize);
                expectThrows(EOFException.class, () -> slice2.seek(beyond));
                assertThat(slice2.getFilePointer(), greaterThanOrEqualTo(0L));
            }
        }
    }

    public void testReadPastEofOnZeroLengthSliceAtEnd() throws IOException {
        // A zero-length slice positioned exactly at the end of the file must throw EOFException on
        // its first read, as the eagerly positioned slice did, for an aligned and an unaligned file
        // end: the deferred fill must fall through to the caller's EOF check rather than surface an
        // empty buffer.
        for (int fileLength : new int[] { BASE_BUFFER_SIZE * 2, BASE_BUFFER_SIZE * 2 + randomIntBetween(1, BASE_BUFFER_SIZE) }) {
            byte[] bytes = new byte[fileLength];
            random().nextBytes(bytes);
            Path path = createTempDir("testDirectIODirectory");
            int blockSize = getBlockSize(path);
            try (Directory dir = new NIOFSDirectory(path)) {
                try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
                    output.writeBytes(bytes, bytes.length);
                }
                try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, BASE_BUFFER_SIZE, 4)) {
                    IndexInput slice = input.slice("empty-at-end", input.length(), 0);
                    expectThrows(EOFException.class, slice::readByte);
                    // and after an explicit seek(0) on the same shape
                    IndexInput slice2 = input.slice("empty-at-end-2", input.length(), 0);
                    slice2.seek(0);
                    expectThrows(EOFException.class, slice2::readByte);
                }
            }
        }
    }

    public void testPrefetchOfPendingSliceStartIsAbsorbed() throws IOException {
        // A hint inside the window a lazily positioned slice reads first names bytes its first fill
        // fetches anyway: it must be absorbed, as the eagerly positioned slice absorbed it because that
        // window was already in its buffer. Issued instead, it would be a full device read on a
        // cache-bypassing input, for a slice that may never be read at all.
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 4 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        // the window is the buffer, one or several blocks
        int bufferSize = blockSize * randomFrom(1, 2, 4);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, bufferSize, 4)) {
                for (long offset : new long[] { 0, 96, blockSize + 96, bufferSize + randomIntBetween(0, blockSize - 1) }) {
                    var slice = (AsyncDirectIOIndexInput) input.slice("test-slice", offset, bytes.length - offset);
                    int pending = Math.toIntExact(offset % blockSize);
                    slice.prefetch(0, 1);
                    // the last byte of the pending window: still inside what the first fill reads
                    slice.prefetch(bufferSize - pending - 1, 1);
                    assertTrue("a hint must not fill the slice at offset " + offset, slice.isDeferred());
                    assertEquals(
                        "a hint for the pending window must not reach the slot table at offset " + offset,
                        0,
                        slice.prefetchSlots()
                    );
                    assertEquals(bytes[(int) offset], slice.readByte());
                    assertEquals(0, slice.prefetchSlots());
                }
            }
            // the top-level input has no pending start and keeps its behavior: the hint is issued
            try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, bufferSize, 4)) {
                input.prefetch(0, 1);
                assertEquals(1, input.prefetchSlots());
                assertEquals(bytes[0], input.readByte());
                assertEquals(0, input.prefetchSlots());
            }
        }
    }

    public void testPrefetchBeyondPendingWindowStillReachesSlots() throws IOException {
        // Only the pending window is absorbed: read-ahead past it is issued, on an unread slice as
        // on a filled one, and the reads that follow are served from those slots.
        byte[] bytes = new byte[BASE_BUFFER_SIZE * 8 + randomIntBetween(1, BASE_BUFFER_SIZE)];
        random().nextBytes(bytes);
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        // the window is the buffer, one or several blocks
        int bufferSize = blockSize * randomFrom(1, 2, 4);
        try (Directory dir = new NIOFSDirectory(path)) {
            try (var output = dir.createOutput("test", IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("test"), blockSize, bufferSize, 4)) {
                long offset = randomFrom(0L, 96L, blockSize + 96L);
                int pending = Math.toIntExact(offset % blockSize);
                // the first byte past the pending window
                var slice = (AsyncDirectIOIndexInput) input.slice("test-slice", offset, bytes.length - offset);
                slice.prefetch(bufferSize - pending, 1);
                assertEquals(1, slice.prefetchSlots());
                assertTrue(slice.isDeferred());
                // a hint reaching the end of the third window: the pending window is absorbed, the two beyond it are issued
                var slice2 = (AsyncDirectIOIndexInput) input.slice("test-slice-2", offset, bytes.length - offset);
                slice2.prefetch(0, bufferSize * 3L - pending);
                assertEquals(2, slice2.prefetchSlots());
                assertTrue(slice2.isDeferred());
                byte[] actual = new byte[bufferSize * 3 - pending];
                slice2.readBytes(actual, 0, actual.length);
                assertArrayEquals(Arrays.copyOfRange(bytes, (int) offset, (int) offset + actual.length), actual);
                assertEquals(0, slice2.prefetchSlots());
            }
        }
    }

    public void testIndexedDisiConstructionOnLazySlicesIssuesNoRead() throws IOException {
        // IndexedDISI's constructor hints prefetch(0, 1) on its block slice and on its jump table
        // before anything is read. A sparse vector field builds one per getFloatVectorValues() call,
        // and the kNN query path often never advances it. Both slices are lazily positioned, so
        // constructing the iterator must cost no read at all, and the iterator must still be right.
        int maxDoc = 1 << 18;
        FixedBitSet docs = new FixedBitSet(maxDoc);
        for (int i = 0; i < maxDoc / 50; i++) {
            docs.set(random().nextInt(maxDoc));
        }
        int cardinality = docs.cardinality();
        byte denseRankPower = 9;
        Path path = createTempDir("testDirectIODirectory");
        int blockSize = getBlockSize(path);
        int bufferSize = blockSize * randomFrom(1, 2, 4);
        long offset = 96;
        long length;
        short jumpTableEntryCount;
        try (Directory dir = new NIOFSDirectory(path)) {
            try (IndexOutput out = dir.createOutput("disi", IOContext.DEFAULT)) {
                byte[] header = new byte[(int) offset];
                random().nextBytes(header);
                out.writeBytes(header, header.length);
                jumpTableEntryCount = IndexedDISI.writeBitSet(new BitSetIterator(docs, cardinality), out, denseRankPower);
                length = out.getFilePointer() - offset;
            }
        }
        try (AsyncDirectIOIndexInput input = new AsyncDirectIOIndexInput(path.resolve("disi"), blockSize, bufferSize, 4)) {
            SliceCapturingIndexInput capturing = new SliceCapturingIndexInput(input);
            IndexedDISI disi = new IndexedDISI(capturing, offset, length, jumpTableEntryCount, denseRankPower, cardinality);
            assertEquals("a block slice and a jump table", 2, capturing.slices.size());
            for (AsyncDirectIOIndexInput slice : capturing.slices) {
                assertTrue("constructing the iterator must not fill " + slice, slice.isDeferred());
                assertEquals("constructing the iterator must not issue a read for " + slice, 0, slice.prefetchSlots());
            }
            assertEquals("the parent input must not issue a read either", 0, input.prefetchSlots());
            // the lazy slices still feed the iterator correctly
            BitSetIterator expected = new BitSetIterator(docs, cardinality);
            for (int doc = expected.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = expected.nextDoc()) {
                assertEquals(doc, disi.nextDoc());
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, disi.nextDoc());
        }
    }

    /**
     * Records the slices handed to the caller. IndexedDISI slices its input itself through its public
     * constructor, and the constructor that would accept ready-made slices is package-private in
     * Lucene, so this is the only way to hold on to the slices it creates.
     */
    private static final class SliceCapturingIndexInput extends FilterIndexInput {
        final List<AsyncDirectIOIndexInput> slices = new ArrayList<>();

        SliceCapturingIndexInput(AsyncDirectIOIndexInput in) {
            super("capturing(" + in + ")", in);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            var slice = (AsyncDirectIOIndexInput) in.slice(sliceDescription, offset, length);
            slices.add(slice);
            return slice;
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
