/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Exercises each {@link ChunkCodec} on its own — one chunk written and read back through a {@link Directory}
 * — including the mapped input a segment is normally read from, where a chunk is decompressed straight out
 * of the file rather than through a copy.
 */
public class ChunkCodecTests extends ESTestCase {

    public void testRoundTrip() throws IOException {
        for (ChunkCodec codec : ChunkCodec.values()) {
            assertRoundTrip(codec, repetitive(50_000));
            assertRoundTrip(codec, incompressible(50_000));
            assertRoundTrip(codec, new byte[] { 7 });
        }
    }

    /**
     * The read path takes the compressed bytes as a slice of the mapped file when the input can hand one
     * out, which {@link MMapDirectory} can and the wrapping directories used elsewhere in these tests
     * cannot. Both paths must decode the same chunk identically.
     */
    public void testRoundTripOverMappedInput() throws IOException {
        try (Directory dir = new MMapDirectory(createTempDir())) {
            final byte[] chunk = repetitive(200_000);
            final int stored = write(dir, ChunkCodec.ZSTD, chunk);
            try (IndexInput in = dir.openInput("chunk.bin", IOContext.DEFAULT)) {
                assertTrue("a mapped input must be able to hand out segment slices", IndexInputUtils.canUseSegmentSlices(in));
                final byte[] read = new byte[chunk.length];
                ChunkCodec.ZSTD.newDecompressor().read(in, stored, read, chunk.length);
                assertArrayEquals(chunk, read);
            }
        }
    }

    /**
     * An input that can hand out segment slices but declines a particular range makes the read copy the chunk
     * into the decompressor's reused scratch buffer. That buffer is sized for the largest chunk seen so far, so
     * a smaller chunk read after a larger one must only pass its own bytes to the decoder.
     */
    public void testScratchReusedAcrossChunksWhenSliceDeclined() throws IOException {
        try (Directory dir = newDirectory()) {
            final byte[] large = incompressible(between(20_000, 50_000));
            final byte[] small = repetitive(between(1_000, 5_000));
            final int largeStored;
            final int smallStored;
            try (IndexOutput out = dir.createOutput("chunk.bin", IOContext.DEFAULT)) {
                largeStored = ChunkCodec.ZSTD.newCompressor().write(large, large.length, out);
                smallStored = ChunkCodec.ZSTD.newCompressor().write(small, small.length, out);
            }
            assertThat(smallStored, lessThan(largeStored));
            try (IndexInput in = new DecliningDirectAccessIndexInput("declining", dir.openInput("chunk.bin", IOContext.DEFAULT))) {
                assertTrue(IndexInputUtils.canUseSegmentSlices(in));
                final ChunkDecompressor decompressor = ChunkCodec.ZSTD.newDecompressor();

                final byte[] readLarge = new byte[large.length];
                decompressor.read(in, largeStored, readLarge, large.length);
                assertArrayEquals(large, readLarge);
                assertThat(in.getFilePointer(), equalTo((long) largeStored));

                final byte[] readSmall = new byte[small.length];
                decompressor.read(in, smallStored, readSmall, small.length);
                assertArrayEquals(small, readSmall);
                assertThat(in.getFilePointer(), equalTo((long) largeStored + smallStored));
            }
        }
    }

    /** A chunk is decoded into a reused buffer, which is usually larger than the chunk it holds. */
    public void testDecodesIntoOversizedBuffer() throws IOException {
        try (Directory dir = newDirectory()) {
            final byte[] chunk = repetitive(20_000);
            final int stored = write(dir, ChunkCodec.ZSTD, chunk);
            try (IndexInput in = dir.openInput("chunk.bin", IOContext.DEFAULT)) {
                final byte[] read = new byte[chunk.length * 3];
                Arrays.fill(read, (byte) 0x7f);
                ChunkCodec.ZSTD.newDecompressor().read(in, stored, read, chunk.length);
                assertArrayEquals(chunk, Arrays.copyOf(read, chunk.length));
                assertEquals("decoding must not write past the chunk", (byte) 0x7f, read[chunk.length]);
            }
        }
    }

    /** A truncated or corrupted chunk must fail rather than hand back a short or wrong buffer. */
    public void testCorruptChunkIsRejected() throws IOException {
        try (Directory dir = newDirectory()) {
            final byte[] chunk = repetitive(10_000);
            final int stored = write(dir, ChunkCodec.ZSTD, chunk);
            try (IndexInput in = dir.openInput("chunk.bin", IOContext.DEFAULT)) {
                final byte[] read = new byte[chunk.length];
                expectThrows(
                    Exception.class,
                    () -> ChunkCodec.ZSTD.newDecompressor().read(in, stored - between(1, stored / 2), read, chunk.length)
                );
            }
        }
    }

    private void assertRoundTrip(ChunkCodec codec, byte[] chunk) throws IOException {
        try (Directory dir = newDirectory()) {
            final int stored = write(dir, codec, chunk);
            try (IndexInput in = dir.openInput("chunk.bin", IOContext.DEFAULT)) {
                final byte[] read = new byte[chunk.length];
                codec.newDecompressor().read(in, stored, read, chunk.length);
                assertArrayEquals("codec " + codec + " over " + chunk.length + " bytes", chunk, read);
            }
        }
    }

    private static int write(Directory dir, ChunkCodec codec, byte[] chunk) throws IOException {
        try (IndexOutput out = dir.createOutput("chunk.bin", IOContext.DEFAULT)) {
            return codec.newCompressor().write(chunk, chunk.length, out);
        }
    }

    private static byte[] repetitive(int length) {
        final StringBuilder builder = new StringBuilder(length + 32);
        for (int i = 0; builder.length() < length; i++) {
            builder.append("host-").append(i % 16).append(".prod.example.com/api/v1/search");
        }
        return Arrays.copyOf(builder.toString().getBytes(StandardCharsets.UTF_8), length);
    }

    private byte[] incompressible(int length) {
        final byte[] bytes = new byte[length];
        random().nextBytes(bytes);
        return bytes;
    }

    /**
     * Implements {@link DirectAccessInput} but never provides a range, so {@link IndexInputUtils#withSlice} reads
     * the bytes into the caller's scratch segment. No real input declines every range; a blob-cache input declines
     * a range that is not cached.
     */
    private static class DecliningDirectAccessIndexInput extends FilterIndexInput implements DirectAccessInput {

        DecliningDirectAccessIndexInput(String resourceDescription, IndexInput delegate) {
            super(resourceDescription, delegate);
        }

        @Override
        public boolean withMemorySegmentSlice(
            long offset,
            long length,
            IntFunction<MemorySegment> scratch,
            CheckedConsumer<MemorySegment, IOException> action
        ) {
            return false;
        }

        @Override
        public boolean withSliceAddresses(
            long[] offsets,
            int length,
            int count,
            MemorySegment addressesScratch,
            CheckedConsumer<MemorySegment, IOException> action
        ) {
            return false;
        }

        @Override
        public IndexInput clone() {
            return new DecliningDirectAccessIndexInput("clone(" + toString() + ")", in.clone());
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new DecliningDirectAccessIndexInput(sliceDescription, in.slice(sliceDescription, offset, length));
        }
    }
}
