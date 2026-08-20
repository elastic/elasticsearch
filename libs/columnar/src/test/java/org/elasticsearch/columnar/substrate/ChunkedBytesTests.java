/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Round-trips a chunked byte stream through a real {@link Directory} for every codec, several chunk targets
 * and several value shapes, checking that a value written at an offset in the uncompressed stream comes back
 * byte for byte — read in order, out of order, and after the reader has been driven elsewhere.
 */
public class ChunkedBytesTests extends ESTestCase {

    private static final List<Integer> CHUNK_TARGETS = List.of(1, 64, 1024, 16 * 1024, 1 << 20);

    public void testEmptyStream() throws IOException {
        for (byte codecId : codecIds()) {
            assertRoundTrip(codecId, randomFrom(CHUNK_TARGETS), List.of());
        }
    }

    public void testAllValuesEmpty() throws IOException {
        // Every value is zero-length, so no chunk is ever written and the reader only sees zero-length reads.
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < between(1, 500); i++) {
            values.add(new byte[0]);
        }
        for (byte codecId : codecIds()) {
            assertRoundTrip(codecId, randomFrom(CHUNK_TARGETS), values);
        }
    }

    public void testSingleValue() throws IOException {
        for (byte codecId : codecIds()) {
            assertRoundTrip(codecId, randomFrom(CHUNK_TARGETS), List.of(bytes("only")));
        }
    }

    public void testRepetitiveValues() throws IOException {
        // The shape a keyword column has: few distinct values, so a codec finds plenty to compress.
        final String[] distinct = { "host-1.prod", "host-2.prod", "/api/v1/search", "/health" };
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            values.add(bytes(distinct[random().nextInt(distinct.length)]));
        }
        for (byte codecId : codecIds()) {
            for (int target : CHUNK_TARGETS) {
                assertRoundTrip(codecId, target, values);
            }
        }
    }

    public void testIncompressibleValues() throws IOException {
        // Random bytes do not compress: the stored chunk can be larger than the chunk it holds, which the
        // reader must still locate and decode from the two tables.
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            final byte[] value = new byte[between(0, 64)];
            random().nextBytes(value);
            values.add(value);
        }
        for (byte codecId : codecIds()) {
            for (int target : CHUNK_TARGETS) {
                assertRoundTrip(codecId, target, values);
            }
        }
    }

    public void testValueLargerThanChunkTarget() throws IOException {
        // A value never splits, so a value past the target simply makes a chunk bigger than the target.
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            final byte[] value = new byte[between(4096, 20000)];
            Arrays.fill(value, (byte) ('a' + (i % 26)));
            values.add(value);
            values.add(bytes("small-" + i));
        }
        for (byte codecId : codecIds()) {
            assertRoundTrip(codecId, 1024, values);
        }
    }

    public void testRandomShapes() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final List<byte[]> values = new ArrayList<>();
            final int count = between(1, 3000);
            for (int i = 0; i < count; i++) {
                final byte[] value = new byte[between(0, 200)];
                for (int b = 0; b < value.length; b++) {
                    // A small alphabet, so the data is compressible but not uniform.
                    value[b] = (byte) ('a' + random().nextInt(8));
                }
                values.add(value);
            }
            assertRoundTrip(randomFrom(codecIds()), randomFrom(CHUNK_TARGETS), values);
        }
    }

    /** A chunk target of zero or less has no valid meaning and must be rejected rather than loop forever. */
    public void testInvalidChunkTargetRejected() throws IOException {
        try (Directory dir = newDirectory(); IndexOutput out = dir.createOutput("chunks.bin", IOContext.DEFAULT)) {
            final int target = randomFrom(0, -1, Integer.MIN_VALUE);
            expectThrows(
                IllegalArgumentException.class,
                () -> new ChunkedBytesWriter(ChunkCodec.forId(ChunkCodec.IDENTITY_ID), target, dir, IOContext.DEFAULT, "chunks", out)
            );
        }
    }

    /** Ids are persisted in column metadata, so they are frozen; an unknown one must fail loudly. */
    public void testFrozenCodecIds() {
        assertEquals((byte) 0, ChunkCodec.IDENTITY_ID);
        assertEquals((byte) 1, ChunkCodec.ZSTD_ID);
        assertTrue("the identity codec stores chunks verbatim", ChunkCodec.forId(ChunkCodec.IDENTITY_ID).isIdentity());
        assertFalse("a compressing codec must not claim to be identity", ChunkCodec.forId(ChunkCodec.ZSTD_ID).isIdentity());
        expectThrows(IllegalArgumentException.class, () -> ChunkCodec.forId((byte) 42));
    }

    /** Compression is the point of a non-identity codec: it must actually shrink repetitive data. */
    public void testZstdCompressesRepetitiveData() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            values.add(bytes("host-" + (i % 8) + ".prod.example.com"));
        }
        final long identity = writeAndMeasure(ChunkCodec.IDENTITY_ID, 64 * 1024, values);
        final long zstd = writeAndMeasure(ChunkCodec.ZSTD_ID, 64 * 1024, values);
        assertTrue("zstd must compress repetitive values, identity=" + identity + " zstd=" + zstd, zstd * 4 < identity);
    }

    /** A larger chunk gives a codec more to work with, so the same values take fewer bytes. */
    public void testLargerChunksCompressBetter() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            values.add(bytes("host-" + (i % 64) + ".prod.example.com/api/v1/search"));
        }
        final long small = writeAndMeasure(ChunkCodec.ZSTD_ID, 1024, values);
        final long large = writeAndMeasure(ChunkCodec.ZSTD_ID, 64 * 1024, values);
        assertTrue("a 64KB chunk must beat a 1KB chunk, small=" + small + " large=" + large, large < small);
    }

    /** The writer stages its chunk index in a temporary file, which must not outlive the write. */
    public void testTemporaryFilesAreRemoved() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            values.add(bytes("value-" + i));
        }
        try (Directory dir = newDirectory()) {
            writeStream(dir, ChunkCodec.ZSTD_ID, 1024, values, new long[values.size() + 1]);
            for (String file : dir.listAll()) {
                assertFalse("a temporary file was left behind: " + file, file.contains("columnar-chunk-index"));
                assertFalse("a temporary file was left behind: " + file, file.contains("columnar-monotonic"));
            }
        }
    }

    /** An aborted write must still clean up after itself. */
    public void testTemporaryFilesAreRemovedWhenUnfinished() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("chunks.bin", IOContext.DEFAULT)) {
                try (
                    ChunkedBytesWriter writer = new ChunkedBytesWriter(
                        ChunkCodec.forId(ChunkCodec.ZSTD_ID),
                        1024,
                        dir,
                        IOContext.DEFAULT,
                        "chunks",
                        out
                    )
                ) {
                    writer.append(bytes("written but never finished"), 0, 26);
                }
            }
            for (String file : dir.listAll()) {
                assertFalse("a temporary file was left behind: " + file, file.contains("columnar-chunk-index"));
            }
        }
    }

    /** Metadata survives serialization, which is how a reader reopens the stream at segment open. */
    public void testMetadataRoundTrips() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            values.add(bytes("value-" + i));
        }
        try (Directory dir = newDirectory()) {
            final long[] offsets = new long[values.size() + 1];
            final ChunkIndexMetadata written = writeStream(dir, ChunkCodec.ZSTD_ID, 4096, values, offsets);
            final byte[] buffer = new byte[1024];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
            written.writeTo(out);
            final ChunkIndexMetadata read = ChunkIndexMetadata.readFrom(new ByteArrayDataInput(buffer, 0, out.getPosition()));
            assertMetadataEquals(written, read);
            assertTrue("a non-empty column has chunks", read.numChunks() > 0);
            assertReads(dir, read, values, offsets, "reopened from serialized metadata");
        }
    }

    /** An empty column serializes to a marker that reopens without touching the data file. */
    public void testEmptyMetadataRoundTrips() throws IOException {
        final byte[] buffer = new byte[64];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        ChunkIndexMetadata.empty().writeTo(out);
        final ChunkIndexMetadata read = ChunkIndexMetadata.readFrom(new ByteArrayDataInput(buffer, 0, out.getPosition()));
        assertMetadataEquals(ChunkIndexMetadata.empty(), read);
        assertEquals(0, read.numChunks());
    }

    /** {@link ChunkIndexMetadata} holds {@code byte[]} table metadata, which a record compares by identity. */
    private static void assertMetadataEquals(ChunkIndexMetadata expected, ChunkIndexMetadata actual) {
        assertEquals("codec id", expected.codecId(), actual.codecId());
        assertEquals("chunk count", expected.numChunks(), actual.numChunks());
        assertEquals("uncompressed length", expected.uncompressedLength(), actual.uncompressedLength());
        assertEquals("data offset", expected.dataOffset(), actual.dataOffset());
        assertEquals("starts offset", expected.startsDataOffset(), actual.startsDataOffset());
        assertEquals("starts length", expected.startsDataLength(), actual.startsDataLength());
        assertArrayEquals("starts metadata", expected.startsMeta(), actual.startsMeta());
        assertEquals("file offsets offset", expected.fileOffsetsDataOffset(), actual.fileOffsetsDataOffset());
        assertEquals("file offsets length", expected.fileOffsetsDataLength(), actual.fileOffsetsDataLength());
        assertArrayEquals("file offsets metadata", expected.fileOffsetsMeta(), actual.fileOffsetsMeta());
    }

    private static List<Byte> codecIds() {
        return List.of(ChunkCodec.IDENTITY_ID, ChunkCodec.ZSTD_ID);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private long writeAndMeasure(byte codecId, int target, List<byte[]> values) throws IOException {
        try (Directory dir = newDirectory()) {
            writeStream(dir, codecId, target, values, new long[values.size() + 1]);
            return dir.fileLength("chunks.bin");
        }
    }

    /**
     * Writes {@code values} as one chunked stream into {@code chunks.bin}, filling {@code offsets} with each
     * value's offset in the uncompressed stream plus a past-the-end marker.
     */
    private ChunkIndexMetadata writeStream(Directory dir, byte codecId, int target, List<byte[]> values, long[] offsets)
        throws IOException {
        try (IndexOutput out = dir.createOutput("chunks.bin", IOContext.DEFAULT)) {
            try (
                ChunkedBytesWriter writer = new ChunkedBytesWriter(ChunkCodec.forId(codecId), target, dir, IOContext.DEFAULT, "chunks", out)
            ) {
                for (int i = 0; i < values.size(); i++) {
                    // A value is what this stream addresses, so a chunk may only end between two of them.
                    writer.boundary();
                    offsets[i] = writer.uncompressedLength();
                    writer.append(values.get(i), 0, values.get(i).length);
                }
                offsets[values.size()] = writer.uncompressedLength();
                return ChunkIndexMetadata.of(writer.finish());
            }
        }
    }

    private void assertRoundTrip(byte codecId, int target, List<byte[]> values) throws IOException {
        final String label = "codec=" + codecId + " chunkTarget=" + target + " values=" + values.size();
        try (Directory dir = newDirectory()) {
            final long[] offsets = new long[values.size() + 1];
            final ChunkIndexMetadata index = writeStream(dir, codecId, target, values, offsets);
            assertReads(dir, index, values, offsets, label);
        }
    }

    private void assertReads(Directory dir, ChunkIndexMetadata index, List<byte[]> values, long[] offsets, String label)
        throws IOException {
        try (IndexInput in = dir.openInput("chunks.bin", IOContext.DEFAULT)) {
            final ChunkedBytesReader reader = index.open(in);
            byte[] scratch = new byte[0];

            // In order: the access pattern a scan uses, and the one the chunk cache is built for.
            for (int i = 0; i < values.size(); i++) {
                scratch = assertValue(reader, scratch, offsets, values, i, label + " in order");
            }

            // Backwards, so every read re-enters a chunk the reader has already left.
            for (int i = values.size() - 1; i >= 0; i--) {
                scratch = assertValue(reader, scratch, offsets, values, i, label + " backwards");
            }

            // Random, so the binary search over chunk starts is exercised from arbitrary positions.
            for (int probe = 0; probe < Math.min(200, values.size()); probe++) {
                final int i = between(0, values.size() - 1);
                scratch = assertValue(reader, scratch, offsets, values, i, label + " random");
            }

            // First and last, the boundaries most likely to be off by one.
            if (values.isEmpty() == false) {
                scratch = assertValue(reader, scratch, offsets, values, 0, label + " first");
                assertValue(reader, scratch, offsets, values, values.size() - 1, label + " last");
            }
        }
    }

    private byte[] assertValue(ChunkedBytesReader reader, byte[] scratch, long[] offsets, List<byte[]> values, int i, String label)
        throws IOException {
        final int length = (int) (offsets[i + 1] - offsets[i]);
        final byte[] read = reader.read(offsets[i], length, scratch);
        assertArrayEquals(label + " value " + i, values.get(i), Arrays.copyOf(read, length));
        return read;
    }
}
