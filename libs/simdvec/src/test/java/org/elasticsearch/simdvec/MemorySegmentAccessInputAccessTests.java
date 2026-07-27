/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for the static {@code withByteBufferSlice} and {@code withByteBufferSlices}
 * helper methods on {@link MemorySegmentAccessInputAccess}.
 */
public class MemorySegmentAccessInputAccessTests extends ESTestCase {

    private static final String FILE_NAME = "test.bin";

    // Converts a random sub-range of an mmap'd file to a ByteBuffer via the helper and
    // verifies the bytes match the original data.
    public void testWithByteBufferSliceReturnsCorrectData() throws Exception {
        byte[] data = randomByteArrayOfLength(512);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertThat(in, instanceOf(MemorySegmentAccessInput.class));
                MemorySegmentAccessInput msai = (MemorySegmentAccessInput) in;

                int offset = randomIntBetween(0, 256);
                int length = randomIntBetween(1, data.length - offset);

                AtomicReference<byte[]> captured = new AtomicReference<>();
                boolean result = MemorySegmentAccessInputAccess.withByteBufferSlice(msai, offset, length, buf -> {
                    assertFalse("buffer should be read-only", buf.hasArray());
                    assertEquals(length, buf.remaining());
                    byte[] bytes = new byte[length];
                    buf.get(bytes);
                    captured.set(bytes);
                });

                assertTrue(result);
                assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), captured.get());
            }
        }
    }

    // Requests the entire file as a single ByteBuffer slice to verify the helper
    // works when offset is 0 and length equals the file size.
    public void testWithByteBufferSliceFullFile() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                MemorySegmentAccessInput msai = (MemorySegmentAccessInput) in;

                AtomicReference<byte[]> captured = new AtomicReference<>();
                boolean result = MemorySegmentAccessInputAccess.withByteBufferSlice(msai, 0, data.length, buf -> {
                    byte[] bytes = new byte[data.length];
                    buf.get(bytes);
                    captured.set(bytes);
                });

                assertTrue(result);
                assertArrayEquals(data, captured.get());
            }
        }
    }

    // Resolves 4 non-contiguous ranges from an mmap'd file via the bulk helper and
    // verifies each returned ByteBuffer contains the correct bytes.
    public void testWithByteBufferSlicesReturnsCorrectData() throws Exception {
        byte[] data = randomByteArrayOfLength(1024);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertThat(in, instanceOf(MemorySegmentAccessInput.class));
                MemorySegmentAccessInput msai = (MemorySegmentAccessInput) in;

                int count = 4;
                int sliceLen = 64;
                long[] offsets = new long[count];
                for (int i = 0; i < count; i++) {
                    offsets[i] = (long) i * sliceLen * 2;
                }

                AtomicReference<byte[][]> captured = new AtomicReference<>();
                boolean result = MemorySegmentAccessInputAccess.withByteBufferSlices(msai, offsets, sliceLen, count, bufs -> {
                    assertEquals(count, bufs.length);
                    byte[][] slices = new byte[count][];
                    for (int i = 0; i < count; i++) {
                        assertFalse("buffer should be read-only", bufs[i].hasArray());
                        assertEquals(sliceLen, bufs[i].remaining());
                        slices[i] = new byte[sliceLen];
                        bufs[i].get(slices[i]);
                    }
                    captured.set(slices);
                });

                assertTrue(result);
                byte[][] slices = captured.get();
                for (int i = 0; i < count; i++) {
                    int off = (int) offsets[i];
                    assertArrayEquals(Arrays.copyOfRange(data, off, off + sliceLen), slices[i]);
                }
            }
        }
    }

    // Exercises the bulk helper with count=1 to verify it degrades correctly to the
    // single-slice case.
    public void testWithByteBufferSlicesSingleSlice() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                MemorySegmentAccessInput msai = (MemorySegmentAccessInput) in;

                int offset = randomIntBetween(0, 128);
                int length = randomIntBetween(1, data.length - offset);
                long[] offsets = { offset };

                AtomicReference<byte[]> captured = new AtomicReference<>();
                boolean result = MemorySegmentAccessInputAccess.withByteBufferSlices(msai, offsets, length, 1, bufs -> {
                    assertEquals(1, bufs.length);
                    byte[] bytes = new byte[length];
                    bufs[0].get(bytes);
                    captured.set(bytes);
                });

                assertTrue(result);
                assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), captured.get());
            }
        }
    }

    // Verifies the ByteBuffer passed to the action is read-only, preventing callers
    // from corrupting the underlying mmap'd memory.
    public void testWithByteBufferSliceBufferIsReadOnly() throws Exception {
        byte[] data = randomByteArrayOfLength(128);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                MemorySegmentAccessInput msai = (MemorySegmentAccessInput) in;

                MemorySegmentAccessInputAccess.withByteBufferSlice(
                    msai,
                    0,
                    64,
                    buf -> { assertTrue("buffer should be read-only", buf.isReadOnly()); }
                );
            }
        }
    }

    // Same as the single-slice read-only check, but for the bulk variant — every
    // buffer in the array must be read-only.
    public void testWithByteBufferSlicesBuffersAreReadOnly() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                MemorySegmentAccessInput msai = (MemorySegmentAccessInput) in;

                long[] offsets = { 0, 64 };
                MemorySegmentAccessInputAccess.withByteBufferSlices(msai, offsets, 32, 2, bufs -> {
                    for (ByteBuffer buf : bufs) {
                        assertTrue("buffer should be read-only", buf.isReadOnly());
                    }
                });
            }
        }
    }

    private static void writeData(Directory dir, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(FILE_NAME, IOContext.DEFAULT)) {
            out.writeBytes(data, 0, data.length);
        }
    }
}
