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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Exercises each {@link ChunkCodec} on its own — one chunk written and read back through a {@link Directory}
 * — including the mapped input a segment is normally read from, where a chunk is decompressed straight out
 * of the file rather than through a copy.
 */
public class ChunkCodecTests extends ESTestCase {

    public void testRoundTrip() throws IOException {
        for (byte codecId : new byte[] { ChunkCodec.IDENTITY_ID, ChunkCodec.ZSTD_ID }) {
            assertRoundTrip(codecId, repetitive(50_000));
            assertRoundTrip(codecId, incompressible(50_000));
            assertRoundTrip(codecId, new byte[] { 7 });
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
            final int stored = write(dir, ChunkCodec.ZSTD_ID, chunk);
            try (IndexInput in = dir.openInput("chunk.bin", IOContext.DEFAULT)) {
                assertTrue("a mapped input must be able to hand out segment slices", IndexInputUtils.canUseSegmentSlices(in));
                final byte[] read = new byte[chunk.length];
                ChunkCodec.forId(ChunkCodec.ZSTD_ID).read(in, stored, read, chunk.length);
                assertArrayEquals(chunk, read);
            }
        }
    }

    /** A chunk is decoded into a reused buffer, which is usually larger than the chunk it holds. */
    public void testDecodesIntoOversizedBuffer() throws IOException {
        try (Directory dir = newDirectory()) {
            final byte[] chunk = repetitive(20_000);
            final int stored = write(dir, ChunkCodec.ZSTD_ID, chunk);
            try (IndexInput in = dir.openInput("chunk.bin", IOContext.DEFAULT)) {
                final byte[] read = new byte[chunk.length * 3];
                Arrays.fill(read, (byte) 0x7f);
                ChunkCodec.forId(ChunkCodec.ZSTD_ID).read(in, stored, read, chunk.length);
                assertArrayEquals(chunk, Arrays.copyOf(read, chunk.length));
                assertEquals("decoding must not write past the chunk", (byte) 0x7f, read[chunk.length]);
            }
        }
    }

    /** A truncated or corrupted chunk must fail rather than hand back a short or wrong buffer. */
    public void testCorruptChunkIsRejected() throws IOException {
        try (Directory dir = newDirectory()) {
            final byte[] chunk = repetitive(10_000);
            final int stored = write(dir, ChunkCodec.ZSTD_ID, chunk);
            try (IndexInput in = dir.openInput("chunk.bin", IOContext.DEFAULT)) {
                final byte[] read = new byte[chunk.length];
                expectThrows(
                    Exception.class,
                    () -> ChunkCodec.forId(ChunkCodec.ZSTD_ID).read(in, stored - between(1, stored / 2), read, chunk.length)
                );
            }
        }
    }

    private void assertRoundTrip(byte codecId, byte[] chunk) throws IOException {
        try (Directory dir = newDirectory()) {
            final int stored = write(dir, codecId, chunk);
            try (IndexInput in = dir.openInput("chunk.bin", IOContext.DEFAULT)) {
                final byte[] read = new byte[chunk.length];
                ChunkCodec.forId(codecId).read(in, stored, read, chunk.length);
                assertArrayEquals("codec " + codecId + " over " + chunk.length + " bytes", chunk, read);
            }
        }
    }

    private static int write(Directory dir, byte codecId, byte[] chunk) throws IOException {
        try (IndexOutput out = dir.createOutput("chunk.bin", IOContext.DEFAULT)) {
            return ChunkCodec.forId(codecId).write(chunk, chunk.length, out);
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
}
