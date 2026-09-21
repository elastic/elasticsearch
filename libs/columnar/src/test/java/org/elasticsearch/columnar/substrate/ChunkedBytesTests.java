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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Round-trips a chunked byte stream through a real {@link Directory} for every codec, several chunk targets
 * and several value shapes, checking that a value written at an offset in the uncompressed stream comes back
 * byte for byte — read in order, out of order, and after the reader has been driven elsewhere.
 */
public class ChunkedBytesTests extends ESTestCase {

    private static final List<Integer> CHUNK_TARGETS = List.of(1, 64, 1024, 16 * 1024, 1 << 20);

    public void testEmptyStream() throws IOException {
        for (ChunkCodec codec : codecs()) {
            assertRoundTrip(codec, randomFrom(CHUNK_TARGETS), List.of());
        }
    }

    public void testAllValuesEmpty() throws IOException {
        // Every value is zero-length, so no chunk is ever written and the reader only sees zero-length reads.
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < between(1, 500); i++) {
            values.add(new byte[0]);
        }
        for (ChunkCodec codec : codecs()) {
            assertRoundTrip(codec, randomFrom(CHUNK_TARGETS), values);
        }
    }

    public void testSingleValue() throws IOException {
        for (ChunkCodec codec : codecs()) {
            assertRoundTrip(codec, randomFrom(CHUNK_TARGETS), List.of(bytes("only")));
        }
    }

    public void testRepetitiveValues() throws IOException {
        // The shape a keyword column has: few distinct values, so a codec finds plenty to compress.
        final String[] distinct = { "host-1.prod", "host-2.prod", "/api/v1/search", "/health" };
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            values.add(bytes(distinct[random().nextInt(distinct.length)]));
        }
        for (ChunkCodec codec : codecs()) {
            for (int target : CHUNK_TARGETS) {
                assertRoundTrip(codec, target, values);
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
        for (ChunkCodec codec : codecs()) {
            for (int target : CHUNK_TARGETS) {
                assertRoundTrip(codec, target, values);
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
        for (ChunkCodec codec : codecs()) {
            assertRoundTrip(codec, 1024, values);
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
            assertRoundTrip(randomFrom(codecs()), randomFrom(CHUNK_TARGETS), values);
        }
    }

    /** A bound of zero or less has no valid meaning and must be rejected rather than loop forever. */
    public void testInvalidChunkBoundsRejected() {
        final int invalid = randomFrom(0, -1, Integer.MIN_VALUE);
        expectThrows(IllegalArgumentException.class, () -> new ChunkBounds(invalid, 1024));
        expectThrows(IllegalArgumentException.class, () -> new ChunkBounds(1024, invalid));
    }

    /** Ids are persisted in column metadata, so they are frozen; an unknown one must fail loudly. */
    public void testFrozenCodecIds() {
        assertEquals((byte) 0, ChunkCodec.IDENTITY.id());
        assertEquals((byte) 1, ChunkCodec.ZSTD.id());
        assertTrue("the identity codec stores chunks verbatim", ChunkCodec.IDENTITY.isIdentity());
        assertFalse("a compressing codec must not claim to be identity", ChunkCodec.ZSTD.isIdentity());
        expectThrows(IllegalArgumentException.class, () -> ChunkCodec.forId((byte) 42));
    }

    /** Compression is the point of a non-identity codec: it must actually shrink repetitive data. */
    public void testZstdCompressesRepetitiveData() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            values.add(bytes("host-" + (i % 8) + ".prod.example.com"));
        }
        final long identity = writeAndMeasure(ChunkCodec.IDENTITY, 64 * 1024, values);
        final long zstd = writeAndMeasure(ChunkCodec.ZSTD, 64 * 1024, values);
        assertTrue("zstd must compress repetitive values, identity=" + identity + " zstd=" + zstd, zstd * 4 < identity);
    }

    /** A larger chunk gives a codec more to work with, so the same values take fewer bytes. */
    public void testLargerChunksCompressBetter() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            values.add(bytes("host-" + (i % 64) + ".prod.example.com/api/v1/search"));
        }
        final long small = writeAndMeasure(ChunkCodec.ZSTD, 1024, values);
        final long large = writeAndMeasure(ChunkCodec.ZSTD, 64 * 1024, values);
        assertTrue("a 64KB chunk must beat a 1KB chunk, small=" + small + " large=" + large, large < small);
    }

    /**
     * Many threads reading one stream at once, each through its own reader. A segment is read concurrently,
     * so anything a reader carries — its buffers, its position in the input — has to be its own. Nothing
     * here is shared but the metadata and the file.
     */
    public void testConcurrentReaders() throws Exception {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            values.add(bytes("value-" + i + "-" + "abcdefghij".repeat(i % 7)));
        }
        try (Directory dir = newDirectory()) {
            final long[] offsets = new long[values.size() + 1];
            final ChunkIndexMetadata index = writeStream(dir, ChunkCodec.ZSTD, 16 * 1024, values, offsets);
            try (IndexInput in = dir.openInput("chunks.bin", IOContext.DEFAULT)) {
                final int threads = 8;
                final CountDownLatch start = new CountDownLatch(1);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final List<Thread> workers = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final int seed = t;
                    final Thread worker = new Thread(() -> {
                        try {
                            final ChunkedBytesReader reader = index.open(in);
                            final Random random = new Random(seed);
                            start.await();
                            byte[] scratch = new byte[0];
                            for (int probe = 0; probe < 4000; probe++) {
                                final int i = random.nextInt(values.size());
                                final int length = (int) (offsets[i + 1] - offsets[i]);
                                scratch = reader.read(offsets[i], length, scratch);
                                final byte[] actual = Arrays.copyOf(scratch, length);
                                if (Arrays.equals(values.get(i), actual) == false) {
                                    throw new AssertionError("thread " + seed + " read value " + i + " wrongly");
                                }
                            }
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        }
                    });
                    workers.add(worker);
                    worker.start();
                }
                start.countDown();
                for (Thread worker : workers) {
                    worker.join();
                }
                if (failure.get() != null) {
                    throw new AssertionError("a concurrent reader failed", failure.get());
                }
            }
        }
    }

    /**
     * A corrupted stream must fail rather than mislead. What it must never do is hang or read past its
     * bounds: a length taken from damaged bytes is still used to size a buffer and to walk a chunk.
     */
    public void testCorruptedStreamFailsCleanly() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            values.add(bytes("value-" + i));
        }
        for (int iteration = 0; iteration < 30; iteration++) {
            try (Directory dir = newDirectory()) {
                final long[] offsets = new long[values.size() + 1];
                final ChunkIndexMetadata index = writeStream(dir, ChunkCodec.ZSTD, 4096, values, offsets);
                final long length = dir.fileLength("chunks.bin");
                final byte[] file = new byte[Math.toIntExact(length)];
                try (IndexInput in = dir.openInput("chunks.bin", IOContext.DEFAULT)) {
                    in.readBytes(file, 0, file.length);
                }
                file[between(0, file.length - 1)] ^= (byte) (1 << between(0, 7));
                try (IndexOutput out = dir.createOutput("corrupt.bin", IOContext.DEFAULT)) {
                    out.writeBytes(file, 0, file.length);
                }
                try (IndexInput in = dir.openInput("corrupt.bin", IOContext.DEFAULT)) {
                    final ChunkedBytesReader reader = index.open(in);
                    byte[] scratch = new byte[0];
                    for (int i = 0; i < values.size(); i++) {
                        final int span = (int) (offsets[i + 1] - offsets[i]);
                        try {
                            scratch = reader.read(offsets[i], span, scratch);
                        } catch (IOException | RuntimeException | AssertionError expected) {
                            // Reporting the damage is the correct outcome; reading on quietly is not.
                            break;
                        }
                    }
                }
            }
        }
    }

    /** A stream that holds no bytes writes no chunk index, since there is no chunk for one to locate. */
    public void testEmptyStreamWritesNoIndex() throws IOException {
        for (ChunkCodec codec : ChunkCodec.values()) {
            try (Directory dir = newDirectory()) {
                final ChunkIndexMetadata index = writeStream(dir, codec, 1024, List.of(), new long[1]);
                assertEquals("no chunks", 0, index.numChunks());
                assertEquals("no starts table", 0, index.startsDataLength());
                assertEquals("no offsets table", 0, index.fileOffsetsDataLength());
            }
        }
    }

    /** Readers seek the input they were given, so each must hold its own and not disturb the others. */
    public void testReadersDoNotDisturbEachOther() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            values.add(bytes("value-" + i + "-" + "x".repeat(i % 40)));
        }
        try (Directory dir = newDirectory()) {
            final long[] offsets = new long[values.size() + 1];
            final ChunkIndexMetadata index = writeStream(dir, ChunkCodec.ZSTD, 4096, values, offsets);
            try (IndexInput in = dir.openInput("chunks.bin", IOContext.DEFAULT)) {
                final ChunkedBytesReader first = index.open(in);
                final ChunkedBytesReader second = index.open(in);
                byte[] a = new byte[0];
                byte[] b = new byte[0];
                // Interleaved, and from opposite ends, so one reader's seeks would derail the other's.
                for (int i = 0; i < values.size(); i++) {
                    final int j = values.size() - 1 - i;
                    final int lengthA = (int) (offsets[i + 1] - offsets[i]);
                    final int lengthB = (int) (offsets[j + 1] - offsets[j]);
                    a = first.read(offsets[i], lengthA, a);
                    b = second.read(offsets[j], lengthB, b);
                    assertArrayEquals("forward " + i, values.get(i), Arrays.copyOf(a, lengthA));
                    assertArrayEquals("backward " + j, values.get(j), Arrays.copyOf(b, lengthB));
                }
            }
        }
    }

    /** The writer stages its chunk index in a temporary file, which must not outlive the write. */
    public void testTemporaryFilesAreRemoved() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            values.add(bytes("value-" + i));
        }
        try (Directory dir = newDirectory()) {
            writeStream(dir, ChunkCodec.ZSTD, 1024, values, new long[values.size() + 1]);
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
                        ChunkCodec.ZSTD,
                        ChunkBounds.ofBytes(1024),
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
            final ChunkIndexMetadata written = writeStream(dir, ChunkCodec.ZSTD, 4096, values, offsets);
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

    private static List<ChunkCodec> codecs() {
        return List.of(ChunkCodec.IDENTITY, ChunkCodec.ZSTD);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private long writeAndMeasure(ChunkCodec codec, int target, List<byte[]> values) throws IOException {
        try (Directory dir = newDirectory()) {
            writeStream(dir, codec, target, values, new long[values.size() + 1]);
            return dir.fileLength("chunks.bin");
        }
    }

    /**
     * Writes {@code values} as one chunked stream into {@code chunks.bin}, filling {@code offsets} with each
     * value's offset in the uncompressed stream plus a past-the-end marker.
     */
    private ChunkIndexMetadata writeStream(Directory dir, ChunkCodec codec, int target, List<byte[]> values, long[] offsets)
        throws IOException {
        return writeStream(dir, codec, ChunkBounds.ofBytes(target), values, offsets);
    }

    private ChunkIndexMetadata writeStream(Directory dir, ChunkCodec codec, ChunkBounds bounds, List<byte[]> values, long[] offsets)
        throws IOException {
        return writeStream(dir, codec, bounds, values, offsets, 1);
    }

    /** As above, offering a boundary every {@code perGroup} values rather than between every two of them. */
    private ChunkIndexMetadata writeStream(
        Directory dir,
        ChunkCodec codec,
        ChunkBounds bounds,
        List<byte[]> values,
        long[] offsets,
        int perGroup
    ) throws IOException {
        try (IndexOutput out = dir.createOutput("chunks.bin", IOContext.DEFAULT)) {
            try (ChunkedBytesWriter writer = new ChunkedBytesWriter(codec, bounds, dir, IOContext.DEFAULT, "chunks", out)) {
                for (int i = 0; i < values.size(); i++) {
                    if (i % perGroup == 0) {
                        // A group is what this stream addresses, so a chunk may only end between two of them.
                        writer.boundary(Math.min(perGroup, values.size() - i));
                    }
                    offsets[i] = writer.uncompressedLength();
                    writer.append(values.get(i), 0, values.get(i).length);
                }
                offsets[values.size()] = writer.uncompressedLength();
                return ChunkIndexMetadata.of(writer.finish());
            }
        }
    }

    /**
     * {@code span} points at bytes where they already are rather than copying them: inside the decoded chunk
     * under a compressing codec, and inside a buffer the reader fills from the file under the identity one,
     * where there is no decoded chunk to point into.
     */
    public void testSpanPointsAtTheBytes() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < between(50, 400); i++) {
            values.add(bytes("value-" + i + "-" + "x".repeat(between(0, 60))));
        }
        for (ChunkCodec codec : codecs()) {
            for (int target : List.of(64, 1024, 16 * 1024)) {
                try (Directory dir = newDirectory()) {
                    final long[] offsets = new long[values.size() + 1];
                    final ChunkIndexMetadata index = writeStream(dir, codec, target, values, offsets);
                    try (IndexInput in = dir.openInput("chunks.bin", IOContext.DEFAULT)) {
                        final ChunkedBytesReader reader = index.open(in);
                        final BytesRef span = new BytesRef();
                        final String label = "codec=" + codec + " target=" + target;
                        for (int i = 0; i < values.size(); i++) {
                            final int length = (int) (offsets[i + 1] - offsets[i]);
                            reader.span(offsets[i], length, span);
                            assertEquals(label + " length at " + i, length, span.length);
                            assertArrayEquals(
                                label + " bytes at " + i,
                                values.get(i),
                                Arrays.copyOfRange(span.bytes, span.offset, span.offset + span.length)
                            );
                        }
                        // Backwards too, so a span re-enters a chunk the reader has already left.
                        for (int i = values.size() - 1; i >= 0; i--) {
                            final int length = (int) (offsets[i + 1] - offsets[i]);
                            reader.span(offsets[i], length, span);
                            assertArrayEquals(
                                label + " bytes backwards at " + i,
                                values.get(i),
                                Arrays.copyOfRange(span.bytes, span.offset, span.offset + span.length)
                            );
                        }
                    }
                }
            }
        }
    }

    /** A span of nothing is an empty reference, and asks the reader for no chunk at all. */
    public void testSpanOfZeroLength() throws IOException {
        for (ChunkCodec codec : codecs()) {
            try (Directory dir = newDirectory()) {
                final List<byte[]> values = List.of(bytes("a"), bytes("b"));
                final long[] offsets = new long[values.size() + 1];
                final ChunkIndexMetadata index = writeStream(dir, codec, 64, values, offsets);
                try (IndexInput in = dir.openInput("chunks.bin", IOContext.DEFAULT)) {
                    final BytesRef span = new BytesRef("untouched");
                    index.open(in).span(0, 0, span);
                    assertEquals("codec=" + codec, 0, span.length);
                }
            }
        }
    }

    /**
     * The buffer a span uses under the identity codec is grown by the longest span asked for and then reused,
     * so a long span followed by a short one leaves the short one reading its own bytes and not the tail of
     * the long one.
     */
    public void testSpanBufferIsReusedAcrossLengths() throws IOException {
        final List<byte[]> values = List.of(bytes("x".repeat(4000)), bytes("tiny"), bytes("y".repeat(2000)), bytes("z"));
        for (ChunkCodec codec : codecs()) {
            try (Directory dir = newDirectory()) {
                final long[] offsets = new long[values.size() + 1];
                final ChunkIndexMetadata index = writeStream(dir, codec, 64 * 1024, values, offsets);
                try (IndexInput in = dir.openInput("chunks.bin", IOContext.DEFAULT)) {
                    final ChunkedBytesReader reader = index.open(in);
                    final BytesRef span = new BytesRef();
                    for (int i = 0; i < values.size(); i++) {
                        final int length = (int) (offsets[i + 1] - offsets[i]);
                        reader.span(offsets[i], length, span);
                        assertArrayEquals(
                            "codec=" + codec + " value " + i,
                            values.get(i),
                            Arrays.copyOfRange(span.bytes, span.offset, span.offset + span.length)
                        );
                    }
                }
            }
        }
    }

    /**
     * Zero-length values reach the value bound while the chunk holds no bytes, and a chunk with no bytes is
     * nothing to decompress, so none is written however low the bound is.
     */
    public void testAValueBoundWritesNoEmptyChunks() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            values.add(new byte[0]);
        }
        for (int maxValues : new int[] { 1, 2, 128 }) {
            for (ChunkCodec codec : codecs()) {
                try (Directory dir = newDirectory()) {
                    final long[] offsets = new long[values.size() + 1];
                    final ChunkIndexMetadata index = writeStream(dir, codec, new ChunkBounds(1 << 20, maxValues), values, offsets);
                    assertEquals("chunks at maxValues=" + maxValues, 0, index.numChunks());
                    assertReads(dir, index, values, offsets, "empty values maxValues=" + maxValues);
                }
            }
        }
    }

    /**
     * A run of zero-length values followed by one that carries bytes: the run closes no chunk, and the chunk
     * the value lands in is the first one written.
     */
    public void testEmptyValuesBeforeARealOneShareItsChunk() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            values.add(new byte[0]);
        }
        values.add(bytes("the only bytes in the stream"));
        for (ChunkCodec codec : codecs()) {
            try (Directory dir = newDirectory()) {
                final long[] offsets = new long[values.size() + 1];
                final ChunkIndexMetadata index = writeStream(dir, codec, new ChunkBounds(1 << 20, 8), values, offsets);
                assertEquals("one chunk for the one value that has bytes", 1, index.numChunks());
                assertReads(dir, index, values, offsets, "empty values then a real one");
            }
        }
    }

    /** A bound of one value closes a chunk after every value, and never before the first one. */
    public void testAValueBoundOfOne() throws IOException {
        final List<byte[]> values = List.of(bytes("a"), bytes("bb"), bytes("ccc"));
        for (ChunkCodec codec : codecs()) {
            try (Directory dir = newDirectory()) {
                final long[] offsets = new long[values.size() + 1];
                final ChunkIndexMetadata index = writeStream(dir, codec, new ChunkBounds(1 << 20, 1), values, offsets);
                assertEquals("one chunk a value", values.size(), index.numChunks());
                assertReads(dir, index, values, offsets, "value bound of one");
            }
        }
    }

    /**
     * Values short enough that the byte target is never reached, so the value bound is the only thing that
     * closes a chunk and the stream is cut where it says.
     */
    public void testTheValueBoundClosesChunksTheByteTargetNeverWould() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            values.add(bytes("v" + i));
        }
        for (int maxValues : new int[] { 1, 2, 7, 128, 999, 1000, 1001 }) {
            final ChunkBounds bounds = new ChunkBounds(1 << 20, maxValues);
            for (ChunkCodec codec : codecs()) {
                try (Directory dir = newDirectory()) {
                    final long[] offsets = new long[values.size() + 1];
                    final ChunkIndexMetadata index = writeStream(dir, codec, bounds, values, offsets);
                    final int expected = (values.size() + maxValues - 1) / maxValues;
                    assertEquals("chunks at maxValues=" + maxValues + " codec=" + codec, expected, index.numChunks());
                    assertReads(dir, index, values, offsets, "codec=" + codec + " maxValues=" + maxValues);
                }
            }
        }
    }

    /** Whichever bound a chunk reaches first closes it, so a stream that reaches both is cut by the nearer one. */
    public void testWhicheverBoundComesFirstCloses() throws IOException {
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            values.add(bytes(randomAlphaOfLength(100)));
        }
        // 100 bytes a value: the byte target lands every 10, well before the value bound.
        final ChunkBounds bytesFirst = new ChunkBounds(1000, 1000);
        // The same values under a value bound that lands every 4, well before the byte target.
        final ChunkBounds valuesFirst = new ChunkBounds(1 << 20, 4);
        try (Directory dir = newDirectory()) {
            final long[] offsets = new long[values.size() + 1];
            final ChunkIndexMetadata byBytes = writeStream(dir, ChunkCodec.IDENTITY, bytesFirst, values, offsets);
            assertEquals("cut by bytes", 40, byBytes.numChunks());
            assertReads(dir, byBytes, values, offsets, "cut by bytes");
        }
        try (Directory dir = newDirectory()) {
            final long[] offsets = new long[values.size() + 1];
            final ChunkIndexMetadata byValues = writeStream(dir, ChunkCodec.IDENTITY, valuesFirst, values, offsets);
            assertEquals("cut by values", 100, byValues.numChunks());
            assertReads(dir, byValues, values, offsets, "cut by values");
        }
    }

    /** The value bound is a maximum: a chunk never covers more values than it allows. */
    public void testAChunkNeverHoldsMoreThanTheValueBound() throws IOException {
        final int perGroup = 8;
        final int groups = 50;
        final List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < perGroup * groups; i++) {
            values.add(bytes("v" + i));
        }
        for (int maxValues : new int[] { 8, 12, 16, 64 }) {
            try (Directory dir = newDirectory()) {
                final long[] offsets = new long[values.size() + 1];
                final ChunkIndexMetadata index = writeStream(
                    dir,
                    ChunkCodec.IDENTITY,
                    new ChunkBounds(1 << 20, maxValues),
                    values,
                    offsets,
                    perGroup
                );
                // Groups are taken whole, so a chunk holds as many as fit under the bound.
                final int perChunk = (maxValues / perGroup) * perGroup;
                assertEquals("chunks at maxValues=" + maxValues, (values.size() + perChunk - 1) / perChunk, index.numChunks());
                assertReads(dir, index, values, offsets, "grouped maxValues=" + maxValues);
            }
        }
    }

    /**
     * The byte bound is a maximum too, which it can only be if a chunk may end anywhere — including inside a
     * value. A stream of a known size therefore takes exactly as many chunks as the bound divides it into.
     */
    public void testNoChunkIsLargerThanTheByteBound() throws IOException {
        for (int valueLength : new int[] { 1, 7, 64, 1000 }) {
            final List<byte[]> values = new ArrayList<>();
            long total = 0;
            for (int i = 0; i < 200; i++) {
                values.add(bytes(randomAlphaOfLength(valueLength)));
                total += valueLength;
            }
            for (int target : new int[] { 16, 64, 512, 4096 }) {
                for (ChunkCodec codec : codecs()) {
                    try (Directory dir = newDirectory()) {
                        final long[] offsets = new long[values.size() + 1];
                        final ChunkIndexMetadata index = writeStream(dir, codec, ChunkBounds.ofBytes(target), values, offsets);
                        assertEquals(
                            "valueLength=" + valueLength + " target=" + target + " codec=" + codec,
                            (total + target - 1) / target,
                            index.numChunks()
                        );
                        assertReads(dir, index, values, offsets, "valueLength=" + valueLength + " target=" + target);
                    }
                }
            }
        }
    }

    /**
     * A value larger than a whole chunk is spread over as many as it takes rather than growing one to fit,
     * and comes back whole from the pieces.
     */
    public void testAValueLargerThanAChunkIsSplitAcrossThem() throws IOException {
        final int target = 1024;
        for (int valueLength : new int[] { 1025, 4096, 10_000 }) {
            final List<byte[]> values = List.of(bytes("before"), bytes(randomAlphaOfLength(valueLength)), bytes("after"));
            long total = 0;
            for (byte[] v : values) {
                total += v.length;
            }
            for (ChunkCodec codec : codecs()) {
                try (Directory dir = newDirectory()) {
                    final long[] offsets = new long[values.size() + 1];
                    final ChunkIndexMetadata index = writeStream(dir, codec, ChunkBounds.ofBytes(target), values, offsets);
                    assertEquals("valueLength=" + valueLength, (total + target - 1) / target, index.numChunks());
                    assertReads(dir, index, values, offsets, "oversized value of " + valueLength);
                }
            }
        }
    }

    private void assertRoundTrip(ChunkCodec codec, int target, List<byte[]> values) throws IOException {
        final String label = "codec=" + codec + " chunkTarget=" + target + " values=" + values.size();
        try (Directory dir = newDirectory()) {
            final long[] offsets = new long[values.size() + 1];
            final ChunkIndexMetadata index = writeStream(dir, codec, target, values, offsets);
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
