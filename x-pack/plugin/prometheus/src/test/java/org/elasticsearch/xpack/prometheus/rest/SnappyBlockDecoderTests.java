/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.compression.Snappy;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class SnappyBlockDecoderTests extends ESTestCase {

    private final SnappyBlockDecoder decoder = new SnappyBlockDecoder(BytesRefRecycler.NON_RECYCLING_INSTANCE);

    public void testDecodeLiteralOnly() throws IOException {
        byte[] original = randomByteArrayOfLength(between(1, 1000));
        byte[] compressed = snappyEncode(original);

        try (var result = decoder.process(new ReleasableBytesReference(new BytesArray(compressed), () -> {}), original.length)) {
            assertEquals(original.length, result.length());
            assertArrayEquals(original, BytesReference.toBytes(result));
        }
    }

    public void testDecodeEmpty() throws IOException {
        // Snappy block: preamble = 0 (empty)
        byte[] compressed = new byte[] { 0 };
        try (var result = decoder.process(new ReleasableBytesReference(new BytesArray(compressed), () -> {}), 1024)) {
            assertEquals(0, result.length());
        }
    }

    public void testDecodeRejectsOversizedOutput() {
        byte[] original = randomByteArrayOfLength(200);
        byte[] compressed = snappyEncode(original);

        var ex = expectThrows(Exception.class, () -> {
            try (var result = decoder.process(new ReleasableBytesReference(new BytesArray(compressed), () -> {}), 100)) {
                // should not reach here
            }
        });
        assertTrue(ex.getMessage().contains("exceeds maximum"));
    }

    public void testDecodeWithBackReference() throws IOException {
        // Create data with repetition to trigger back-references
        byte[] original = new byte[256];
        byte[] pattern = randomByteArrayOfLength(8);
        for (int i = 0; i < original.length; i++) {
            original[i] = pattern[i % pattern.length];
        }

        byte[] compressed = snappyEncode(original);
        // Compressed should be smaller due to repetition
        assertTrue("expected compression, got " + compressed.length + " >= " + original.length, compressed.length < original.length);

        try (var result = decoder.process(new ReleasableBytesReference(new BytesArray(compressed), () -> {}), original.length)) {
            assertEquals(original.length, result.length());
            assertArrayEquals(original, BytesReference.toBytes(result));
        }
    }

    public void testDecodeReleasesInput() throws IOException {
        byte[] original = randomByteArrayOfLength(64);
        byte[] compressed = snappyEncode(original);
        var input = new ReleasableBytesReference(new BytesArray(compressed), () -> {});

        try (var result = decoder.process(input, 1024)) {
            assertFalse("input should be released after process()", input.hasReferences());
        }
    }

    public void testDecodeMalformedInput() {
        // Preamble says 10 bytes uncompressed, then a COPY_4_BYTE_OFFSET tag with insufficient data
        byte[] garbage = new byte[] { 10, (byte) 0xFF };
        expectThrows(IOException.class, () -> {
            try (var result = decoder.process(new ReleasableBytesReference(new BytesArray(garbage), () -> {}), 1024)) {
                // should not reach here
            }
        });
    }

    public void testDecodeEmptyInput() {
        expectThrows(IOException.class, () -> {
            try (var result = decoder.process(new ReleasableBytesReference(BytesArray.EMPTY, () -> {}), 1024)) {
                // should not reach here
            }
        });
    }

    public void testDecodeRoundTripsVariousSizes() throws IOException {
        for (int size : new int[] { 1, 15, 16, 60, 61, 255, 256, 4096, 16384 }) {
            byte[] original = randomByteArrayOfLength(size);
            byte[] compressed = snappyEncode(original);
            try (var result = decoder.process(new ReleasableBytesReference(new BytesArray(compressed), () -> {}), size + 1024)) {
                assertEquals(size, result.length());
                assertArrayEquals("failed for size " + size, original, BytesReference.toBytes(result));
            }
        }
    }

    public void testDecodeSpansMultiplePages() throws IOException {
        // 48 KiB of repeating pattern exercises page boundaries (page = 16 KiB) and back-references
        int size = 48 * 1024;
        byte[] original = new byte[size];
        byte[] pattern = randomByteArrayOfLength(between(4, 64));
        for (int i = 0; i < size; i++) {
            original[i] = pattern[i % pattern.length];
        }
        byte[] compressed = snappyEncode(original);
        assertTrue("expected compression", compressed.length < original.length);
        try (var result = decoder.process(new ReleasableBytesReference(new BytesArray(compressed), () -> {}), size)) {
            assertEquals(size, result.length());
            assertArrayEquals(original, BytesReference.toBytes(result));
        }
    }

    public void testDecodeLargeRandomDataAcrossPages() throws IOException {
        // Random data > 16 KiB doesn't compress well but exercises multi-page literal writes
        int size = 32 * 1024 + between(1, 1000);
        byte[] original = randomByteArrayOfLength(size);
        byte[] compressed = snappyEncode(original);
        try (var result = decoder.process(new ReleasableBytesReference(new BytesArray(compressed), () -> {}), size + 1024)) {
            assertEquals(size, result.length());
            assertArrayEquals(original, BytesReference.toBytes(result));
        }
    }

    public void testDecodeBackRefAcrossPageBoundary() throws IOException {
        // Place a pattern just before a page boundary, then repeat it across the boundary.
        // Page size is 16384, so we put ~16380 bytes of random data, then a 16-byte pattern,
        // then repeat the pattern several times to create a back-ref that spans pages.
        int prefixLen = 16384 - 16;
        byte[] prefix = randomByteArrayOfLength(prefixLen);
        byte[] pattern = randomByteArrayOfLength(16);
        int repetitions = 20;
        int totalSize = prefixLen + pattern.length * (1 + repetitions);
        byte[] original = new byte[totalSize];
        System.arraycopy(prefix, 0, original, 0, prefixLen);
        for (int i = 0; i <= repetitions; i++) {
            System.arraycopy(pattern, 0, original, prefixLen + i * pattern.length, pattern.length);
        }
        byte[] compressed = snappyEncode(original);
        try (var result = decoder.process(new ReleasableBytesReference(new BytesArray(compressed), () -> {}), totalSize + 1024)) {
            assertEquals(totalSize, result.length());
            assertArrayEquals(original, BytesReference.toBytes(result));
        }
    }

    public void testDecodeWithCompositeInput() throws IOException {
        // Split compressed data across multiple BytesReference chunks to exercise InputCursor chunk transitions
        byte[] original = randomByteArrayOfLength(between(100, 1000));
        byte[] compressed = snappyEncode(original);

        // Split into 3 chunks at arbitrary points
        int split1 = between(1, compressed.length - 2);
        int split2 = between(split1 + 1, compressed.length - 1);
        BytesReference composite = CompositeBytesReference.of(
            new BytesArray(compressed, 0, split1),
            new BytesArray(compressed, split1, split2 - split1),
            new BytesArray(compressed, split2, compressed.length - split2)
        );

        try (var result = decoder.process(new ReleasableBytesReference(composite, () -> {}), original.length + 1024)) {
            assertEquals(original.length, result.length());
            assertArrayEquals(original, BytesReference.toBytes(result));
        }
    }

    public void testDecodeCopy4ByteOffset() throws IOException {
        // Hand-craft a Snappy stream with COPY_4_BYTE_OFFSET (tag type 3).
        // Netty's encoder never produces this tag type, so manual construction is needed.
        // Use an offset > 65535 to exercise the 4-byte offset path specifically.
        int literalLen = 65536;
        byte[] literal = randomByteArrayOfLength(literalLen);
        int copyOffset = literalLen;
        int copyLength = 10;
        int total = literalLen + copyLength;

        var buf = new ByteArrayOutputStream();
        writeVarint(buf, total);
        buf.write(61 << 2); // literal tag, case 61 (2-byte length)
        int encodedLiteralLen = literalLen - 1;
        buf.write(encodedLiteralLen & 0xFF);
        buf.write((encodedLiteralLen >> 8) & 0xFF);
        buf.write(literal);
        buf.write(0x03 | ((copyLength - 1) << 2)); // COPY_4_BYTE_OFFSET tag
        writeIntLE(buf, copyOffset);

        byte[] expected = new byte[total];
        System.arraycopy(literal, 0, expected, 0, literalLen);
        System.arraycopy(literal, 0, expected, literalLen, copyLength);

        byte[] compressed = buf.toByteArray();
        try (var result = decoder.process(releasable(compressed), total)) {
            assertEquals(total, result.length());
            assertArrayEquals(expected, BytesReference.toBytes(result));
        }
    }

    public void testDecodeRunLengthRepetition() throws IOException {
        // Single byte repeated forces copies with offset=1, which triggers
        // the backOffset < length path in selfCopy (run-length encoding).
        byte value = randomByte();
        int size = between(256, 1024);
        byte[] original = new byte[size];
        Arrays.fill(original, value);

        byte[] compressed = snappyEncode(original);
        assertTrue("expected compression", compressed.length < original.length);

        try (var result = decoder.process(releasable(compressed), size)) {
            assertEquals(size, result.length());
            byte[] decoded = BytesReference.toBytes(result);
            for (int i = 0; i < size; i++) {
                assertEquals("mismatch at index " + i, value, decoded[i]);
            }
        }
    }

    public void testDecodeLiteralLengthCase60() throws IOException {
        // Case 60: 1 extra byte encodes literal lengths 61-256
        int length = between(61, 256);
        assertDecodesHandCraftedLiteral(length, 60);
    }

    public void testDecodeLiteralLengthCase61() throws IOException {
        // Case 61: 2 extra bytes encode literal lengths 257-65536
        int length = between(257, 65536);
        assertDecodesHandCraftedLiteral(length, 61);
    }

    public void testDecodeLiteralLengthCase62() throws IOException {
        // Case 62: 3 extra bytes encode literal lengths 65537+
        int length = between(65537, 70000);
        assertDecodesHandCraftedLiteral(length, 62);
    }

    private void assertDecodesHandCraftedLiteral(int length, int caseNum) throws IOException {
        byte[] data = randomByteArrayOfLength(length);
        var buf = new ByteArrayOutputStream();
        writeVarint(buf, length);
        buf.write(caseNum << 2); // literal tag with specified case number
        int encodedLen = length - 1;
        int extraBytes = caseNum - 59; // case 60 → 1 byte, 61 → 2, 62 → 3
        for (int i = 0; i < extraBytes; i++) {
            buf.write((encodedLen >> (i * 8)) & 0xFF);
        }
        buf.write(data);

        byte[] compressed = buf.toByteArray();
        try (var result = decoder.process(releasable(compressed), length)) {
            assertEquals(length, result.length());
            assertArrayEquals(data, BytesReference.toBytes(result));
        }
    }

    public void testDecodeOverflowVarintPreamble() {
        // 5-byte varint that decodes to -1 (0xFFFFFFFF), triggering the negative length check
        byte[] overflow = new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x0F };
        var ex = expectThrows(IOException.class, () -> {
            try (var result = decoder.process(releasable(overflow), Long.MAX_VALUE)) {
                fail("should not reach here");
            }
        });
        assertTrue(ex.getMessage(), ex.getMessage().contains("negative"));
    }

    public void testDecodeTooLargeVarintPreamble() {
        // 5 continuation bytes → shift reaches 35, exceeding the 32-bit limit
        byte[] tooLarge = new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x00 };
        var ex = expectThrows(IOException.class, () -> {
            try (var result = decoder.process(releasable(tooLarge), Long.MAX_VALUE)) {
                fail("should not reach here");
            }
        });
        assertTrue(ex.getMessage(), ex.getMessage().contains("preamble"));
    }

    public void testDecodeReleasesResourcesOnError() {
        var openPages = new AtomicInteger();
        var trackingDecoder = getTrackingDecoder(openPages);

        // Stream claims 40000 bytes but only provides a 20000-byte literal.
        // The decoder allocates 2 pages for the literal, then fails on the length mismatch.
        byte[] literal = randomByteArrayOfLength(20000);
        var buf = new ByteArrayOutputStream();
        writeVarint(buf, 40000);
        buf.write(61 << 2); // literal tag, case 61 (2-byte length)
        int len = 20000 - 1;
        buf.write(len & 0xFF);
        buf.write((len >> 8) & 0xFF);
        try {
            buf.write(literal);
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        byte[] compressed = buf.toByteArray();
        expectThrows(IOException.class, () -> {
            try (var result = trackingDecoder.process(releasable(compressed), 40000)) {
                fail("should not reach here");
            }
        });
        assertEquals("all allocated pages should be released on error", 0, openPages.get());
    }

    private static SnappyBlockDecoder getTrackingDecoder(AtomicInteger openPages) {
        Recycler<BytesRef> tracking = new Recycler<>() {
            @Override
            public V<BytesRef> obtain() {
                openPages.incrementAndGet();
                BytesRef ref = new BytesRef(new byte[pageSize()], 0, pageSize());
                return new V<>() {
                    @Override
                    public BytesRef v() {
                        return ref;
                    }

                    @Override
                    public boolean isRecycled() {
                        return false;
                    }

                    @Override
                    public void close() {
                        openPages.decrementAndGet();
                    }
                };
            }

            @Override
            public int pageSize() {
                return 16384;
            }
        };

        return new SnappyBlockDecoder(tracking);
    }

    private static ReleasableBytesReference releasable(byte[] data) {
        return new ReleasableBytesReference(new BytesArray(data), () -> {});
    }

    private static void writeVarint(ByteArrayOutputStream buf, int value) {
        while ((value & ~0x7F) != 0) {
            buf.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf.write(value);
    }

    private static void writeIntLE(ByteArrayOutputStream buf, int value) {
        buf.write(value & 0xFF);
        buf.write((value >> 8) & 0xFF);
        buf.write((value >> 16) & 0xFF);
        buf.write((value >> 24) & 0xFF);
    }

    /** Encodes data using Netty's Snappy encoder for test round-trip verification. */
    static byte[] snappyEncode(byte[] input) {
        ByteBuf in = Unpooled.wrappedBuffer(input);
        ByteBuf out = Unpooled.buffer(input.length);
        try {
            new Snappy().encode(in, out, input.length);
            byte[] result = new byte[out.readableBytes()];
            out.readBytes(result);
            return result;
        } finally {
            in.release();
            out.release();
        }
    }
}
