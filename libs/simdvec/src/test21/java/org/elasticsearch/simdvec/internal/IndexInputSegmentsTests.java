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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Tests that {@link IndexInputSegments#withSlice} correctly handles all
 * three input types: {@link MemorySegmentAccessInput} (mmap),
 * {@link DirectAccessInput} (byte-buffer), and plain {@link IndexInput}
 * (heap-copy fallback).
 */
public class IndexInputSegmentsTests extends ESTestCase {

    private static final String FILE_NAME = "test.bin";

    // -- withSlice path tests -------------------------------------------------

    public void testWithSliceMemorySegmentAccessInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertTrue(in instanceof MemorySegmentAccessInput);
                verifyWithSlice(in, data);
            }
        }
    }

    public void testWithSliceDirectAccessInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput rawIn = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                IndexInput in = new DirectAccessWrapper("dai", rawIn, data);
                assertTrue(in instanceof DirectAccessInput);
                verifyWithSlice(in, data);
            }
        }
    }

    public void testWithSlicePlainIndexInput() throws Exception {
        byte[] data = randomByteArrayOfLength(256);
        try (Directory dir = new NIOFSDirectory(createTempDir())) {
            writeData(dir, data);
            try (IndexInput in = dir.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertFalse(in instanceof MemorySegmentAccessInput);
                assertFalse(in instanceof DirectAccessInput);
                verifyWithSlice(in, data);
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
                IndexInput in = new DirectAccessWrapper("dai", rawIn, data);
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

    // -- helpers --------------------------------------------------------------

    private void verifyWithSlice(IndexInput in, byte[] expectedData) throws IOException {
        int firstChunk = 64;
        byte[] result1 = IndexInputSegments.withSlice(in, firstChunk, segment -> {
            byte[] buf = new byte[(int) segment.byteSize()];
            MemorySegment.ofArray(buf).copyFrom(segment);
            return buf;
        });
        assertArrayEquals(Arrays.copyOfRange(expectedData, 0, firstChunk), result1);
        assertEquals(firstChunk, in.getFilePointer());

        int secondChunk = 128;
        byte[] result2 = IndexInputSegments.withSlice(in, secondChunk, segment -> {
            byte[] buf = new byte[(int) segment.byteSize()];
            MemorySegment.ofArray(buf).copyFrom(segment);
            return buf;
        });
        assertArrayEquals(Arrays.copyOfRange(expectedData, firstChunk, firstChunk + secondChunk), result2);
        assertEquals(firstChunk + secondChunk, in.getFilePointer());

        int remaining = expectedData.length - firstChunk - secondChunk;
        byte[] result3 = IndexInputSegments.withSlice(in, remaining, segment -> {
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

    /**
     * Wraps an existing IndexInput with DirectAccessInput support,
     * serving byte-buffer slices from the provided data array.
     */
    static class DirectAccessWrapper extends FilterIndexInput implements DirectAccessInput {
        private final byte[] data;

        DirectAccessWrapper(String resourceDescription, IndexInput delegate, byte[] data) {
            super(resourceDescription, delegate);
            this.data = data;
        }

        @Override
        public boolean withByteBufferSlice(long offset, long length, CheckedConsumer<ByteBuffer, IOException> action) throws IOException {
            ByteBuffer bb = ByteBuffer.wrap(data, (int) offset, (int) length).asReadOnlyBuffer();
            action.accept(bb);
            return true;
        }

        @Override
        public IndexInput clone() {
            return new DirectAccessWrapper("clone", in.clone(), data);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new DirectAccessWrapper(sliceDescription, in.slice(sliceDescription, offset, length), data);
        }
    }
}
