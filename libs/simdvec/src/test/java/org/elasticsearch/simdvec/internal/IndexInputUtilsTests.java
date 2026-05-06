/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.common.lucene.store.DirectAccessIndexInput;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Tests that {@link IndexInputUtils#withSlice} correctly handles all
 * three input types: {@link MemorySegmentAccessInput} (mmap),
 * {@link DirectAccessInput} (byte-buffer), and plain {@link IndexInput}
 * (heap-copy fallback).
 */
public class IndexInputUtilsTests extends ESTestCase {

    private static final String FILE_NAME = "test.bin";

    // -- withSlice path tests -------------------------------------------------

    public void testWithSliceMemorySegmentAccessInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertThat(in, instanceOf(MemorySegmentAccessInput.class));
                verifyWithSlice(in, data);
            }
        }
    }

    public void testWithSliceDirectAccessInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput rawIn = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                IndexInput in = new DirectAccessIndexInput("dai", rawIn, data, NativeAccess.instance());
                assertThat(in, instanceOf(DirectAccessInput.class));
                verifyWithSlice(in, data);
            }
        }
    }

    public void testWithSlicePlainIndexInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertThat(in, not(instanceOf(MemorySegmentAccessInput.class)));
                assertThat(in, not(instanceOf(DirectAccessInput.class)));
                verifyWithSlice(in, data);
            }
        }
    }

    public void testPlainInputFallbackProducesNativeSegmentOnJava21() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertThat(in, not(instanceOf(MemorySegmentAccessInput.class)));
                assertThat(in, not(instanceOf(DirectAccessInput.class)));
                IndexInputUtils.withSlice(in, data.length, byte[]::new, segment -> {
                    if (Runtime.version().feature() < 22) {
                        assertTrue("segment should be native-backed on Java 21", segment.heapBase().isEmpty());
                    }
                    byte[] buf = new byte[(int) segment.byteSize()];
                    MemorySegment.ofArray(buf).copyFrom(segment);
                    assertArrayEquals(data, buf);
                    return null;
                });
            }
        }
    }

    // -- constructor validation tests -----------------------------------------

    public void testES92ConstructorAcceptsPlainInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                new MemorySegmentES92Int7VectorsScorer(in, 64, 16);
            }
        }
    }

    public void testES92ConstructorAcceptsMMapInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                new MemorySegmentES92Int7VectorsScorer(in, 64, 16);
            }
        }
    }

    public void testES92ConstructorAcceptsDirectAccessInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput rawIn = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                IndexInput in = new DirectAccessIndexInput("dai", rawIn, data, NativeAccess.instance());
                new MemorySegmentES92Int7VectorsScorer(in, 64, 16);
            }
        }
    }

    public void testES92ConstructorRejectsUnwrappedFilterIndexInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput rawIn = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                IndexInput wrapped = new FilterIndexInput("plain-wrapper", rawIn) {};
                expectThrows(IllegalArgumentException.class, () -> new MemorySegmentES92Int7VectorsScorer(wrapped, 64, 16));
            }
        }
    }

    // -- withSliceAddresses path tests ----------------------------------------

    public void testWithSliceAddressesMemorySegmentAccessInput() throws Exception {
        byte[] data = randomByteArrayOfLength(1024);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertThat(in, instanceOf(MemorySegmentAccessInput.class));
                verifyWithSliceAddresses(in, data, 64);
            }
        }
    }

    public void testWithSliceAddressesDirectAccessInput() throws Exception {
        byte[] data = randomByteArrayOfLength(1024);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput rawIn = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                IndexInput in = new DirectAccessIndexInput("dai", rawIn, data, NativeAccess.instance());
                assertThat(in, instanceOf(DirectAccessInput.class));
                verifyWithSliceAddresses(in, data, 64);
            }
        }
    }

    // Plain IndexInput has no MSAI or DAI — withSliceAddresses should return false.
    public void testWithSliceAddressesReturnsFalseForPlainInput() throws Exception {
        byte[] data = randomByteArrayOfLength(1024);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertThat(in, not(instanceOf(MemorySegmentAccessInput.class)));
                assertThat(in, not(instanceOf(DirectAccessInput.class)));
                long[] offsets = { 0, 64, 128, 192 };
                boolean result = IndexInputUtils.withSliceAddresses(
                    in,
                    offsets,
                    64,
                    4,
                    a -> { fail("action should not be called for plain IndexInput"); }
                );
                assertFalse(result);
            }
        }
    }

    private void verifyWithSliceAddresses(IndexInput in, byte[] expectedData, int sliceLen) throws IOException {
        int count = 4;
        long[] offsets = new long[count];
        for (int i = 0; i < count; i++) {
            offsets[i] = (long) i * sliceLen * 2;
        }
        boolean result = IndexInputUtils.withSliceAddresses(in, offsets, sliceLen, count, a -> {
            for (int i = 0; i < count; i++) {
                MemorySegment addr = a.getAtIndex(ValueLayout.ADDRESS, i);
                assertTrue("address should be non-null", addr != MemorySegment.NULL);
                MemorySegment seg = addr.reinterpret(sliceLen);
                byte[] actual = new byte[sliceLen];
                MemorySegment.ofArray(actual).copyFrom(seg);
                int off = (int) offsets[i];
                assertArrayEquals(Arrays.copyOfRange(expectedData, off, off + sliceLen), actual);
            }
        });
        assertTrue("withSliceAddresses should succeed", result);
    }

    // -- helpers --------------------------------------------------------------

    private void verifyWithSlice(IndexInput in, byte[] expectedData) throws IOException {
        int firstChunk = 64;
        byte[] result1 = IndexInputUtils.withSlice(in, firstChunk, byte[]::new, segment -> {
            byte[] buf = new byte[(int) segment.byteSize()];
            MemorySegment.ofArray(buf).copyFrom(segment);
            return buf;
        });
        assertArrayEquals(Arrays.copyOfRange(expectedData, 0, firstChunk), result1);
        assertEquals(firstChunk, in.getFilePointer());

        int secondChunk = 128;
        byte[] result2 = IndexInputUtils.withSlice(in, secondChunk, byte[]::new, segment -> {
            byte[] buf = new byte[(int) segment.byteSize()];
            MemorySegment.ofArray(buf).copyFrom(segment);
            return buf;
        });
        assertArrayEquals(Arrays.copyOfRange(expectedData, firstChunk, firstChunk + secondChunk), result2);
        assertEquals(firstChunk + secondChunk, in.getFilePointer());

        int remaining = expectedData.length - firstChunk - secondChunk;
        byte[] result3 = IndexInputUtils.withSlice(in, remaining, byte[]::new, segment -> {
            byte[] buf = new byte[(int) segment.byteSize()];
            MemorySegment.ofArray(buf).copyFrom(segment);
            return buf;
        });
        assertArrayEquals(Arrays.copyOfRange(expectedData, firstChunk + secondChunk, expectedData.length), result3);
        assertEquals(expectedData.length, in.getFilePointer());
    }

    private static void writeData(Directory dir, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(FILE_NAME, IOContext.DEFAULT)) {
            out.writeBytes(data, 0, data.length);
        }
    }

}
