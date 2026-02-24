/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.Snappy;
import io.netty.util.ReferenceCountUtil;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for Snappy block decoding behavior, including chunked and truncated input.
 */
public class SnappyBlockDecoderTests extends ESTestCase {

    private static final int MAX_UNCOMPRESSED_SIZE = 32 * 1024;

    public void testDecodesSnappyBlock() {
        byte[] original = randomAlphanumericOfLength(100).getBytes(StandardCharsets.UTF_8);
        byte[] compressed = snappyBlockCompress(original);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(compressed)));
            assertArrayEquals(original, readDecoded(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testDecodesSnappyBlockWhenChunkedByteByByte() {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1024, 8192));
        byte[] compressed = snappyBlockCompress(original);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            for (int i = 0; i < compressed.length - 1; i++) {
                channel.writeInbound(Unpooled.wrappedBuffer(compressed, i, 1));
            }
            assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(compressed, compressed.length - 1, 1)));
            assertArrayEquals(original, readDecoded(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testDecodesSnappyBlockWhenPreambleIsChunked() {
        byte[] original = randomByteArrayOfLength(300);
        byte[] compressed = snappyBlockCompress(original);
        assertTrue("expected multi-byte preamble", (compressed[0] & 0x80) != 0);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            channel.writeInbound(Unpooled.wrappedBuffer(compressed, 0, 1));
            assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(compressed, 1, compressed.length - 1)));
            assertArrayEquals(original, readDecoded(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testDecodesSnappyBlockWhenSplitAcrossChunks() {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1024, 8192));
        byte[] compressed = snappyBlockCompress(original);
        int splitPoint = randomIntBetween(1, compressed.length - 2);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            channel.writeInbound(Unpooled.wrappedBuffer(compressed, 0, splitPoint));
            assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(compressed, splitPoint, compressed.length - splitPoint)));
            assertArrayEquals(original, readDecoded(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testRejectsTruncatedSnappyBlock() {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1024, 8192));
        byte[] compressed = snappyBlockCompress(original);
        byte[] truncated = Arrays.copyOf(compressed, compressed.length - 1);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            // Partial output may be produced incrementally before truncation is detected
            channel.writeInbound(Unpooled.wrappedBuffer(truncated));
            DecompressionException exception = expectThrows(DecompressionException.class, channel::finish);
            assertThat(exception.getMessage(), containsString("truncated snappy block"));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testRejectsTruncatedSnappyPreambleOnFinish() {
        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(new byte[] { (byte) 0x80 })));
            DecompressionException exception = expectThrows(DecompressionException.class, channel::finish);
            assertThat(exception.getMessage(), containsString("truncated snappy preamble"));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testDecodesZeroLengthSnappyBlock() {
        byte[] original = new byte[0];
        byte[] compressed = snappyBlockCompress(original);
        assertArrayEquals(new byte[] { 0 }, compressed);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(compressed)));
            assertArrayEquals(original, readDecoded(channel));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testRejectsSnappyBlockWhenMaxSizeExceeded() {
        int maxUncompressedSize = randomIntBetween(128, 512);
        byte[] original = randomByteArrayOfLength(randomIntBetween(maxUncompressedSize + 1, maxUncompressedSize + 512));
        byte[] compressed = snappyBlockCompress(original);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(maxUncompressedSize));
        try {
            DecompressionException e = expectThrows(
                DecompressionException.class,
                () -> channel.writeInbound(Unpooled.wrappedBuffer(compressed))
            );
            assertThat(e.getMessage(), containsString("exceeds maximum allowed size"));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testRejectsSnappyBlockWithTrailingBytes() {
        byte[] original = randomByteArrayOfLength(randomIntBetween(256, 2048));
        byte[] compressed = snappyBlockCompress(original);
        byte[] malformed = Arrays.copyOf(compressed, compressed.length + 2);
        malformed[compressed.length] = (byte) 0xF0;
        malformed[compressed.length + 1] = (byte) 0x00;

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            DecompressionException exception = expectThrows(
                DecompressionException.class,
                () -> channel.writeInbound(Unpooled.wrappedBuffer(malformed))
            );
            assertThat(exception.getMessage(), containsString("trailing bytes after decompression completed"));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testProducesIncrementalOutputForCompressibleData() {
        // Compressible data produces many small Snappy elements (literals + copies).
        // Netty's Snappy processes complete elements per decode() call, so splitting the
        // compressed stream across chunks yields incremental output at element boundaries.
        byte[] pattern = randomAlphanumericOfLength(64).getBytes(StandardCharsets.UTF_8);
        int repetitions = MAX_UNCOMPRESSED_SIZE / pattern.length;
        byte[] original = new byte[pattern.length * repetitions];
        for (int i = 0; i < repetitions; i++) {
            System.arraycopy(pattern, 0, original, i * pattern.length, pattern.length);
        }
        byte[] compressed = snappyBlockCompress(original);
        assertTrue("expected compressible data to compress well", compressed.length < original.length / 2);

        int splitPoint = randomIntBetween(compressed.length / 4, compressed.length * 3 / 4);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            channel.writeInbound(Unpooled.wrappedBuffer(compressed, 0, splitPoint));
            channel.writeInbound(Unpooled.wrappedBuffer(compressed, splitPoint, compressed.length - splitPoint));

            int messageCount = 0;
            ByteBuf combined = Unpooled.buffer();
            try {
                Object message;
                while ((message = channel.readInbound()) != null) {
                    try {
                        messageCount++;
                        combined.writeBytes((ByteBuf) message);
                    } finally {
                        ReferenceCountUtil.release(message);
                    }
                }
                assertArrayEquals(original, ByteBufUtil.getBytes(combined));
            } finally {
                combined.release();
            }
            assertThat("expected incremental output across chunks", messageCount, greaterThanOrEqualTo(2));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testRejectsSnappyBlockWithOverlongPreamble() {
        byte[] malformed = new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x00 };

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockDecoder(MAX_UNCOMPRESSED_SIZE));
        try {
            DecompressionException e = expectThrows(
                DecompressionException.class,
                () -> channel.writeInbound(Unpooled.wrappedBuffer(malformed))
            );
            assertThat(e.getMessage(), containsString("Preamble is greater than 4 bytes"));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static byte[] readDecoded(EmbeddedChannel channel) {
        ByteBuf combined = Unpooled.buffer();
        try {
            Object message;
            while ((message = channel.readInbound()) != null) {
                try {
                    combined.writeBytes((ByteBuf) message);
                } finally {
                    ReferenceCountUtil.release(message);
                }
            }
            return ByteBufUtil.getBytes(combined);
        } finally {
            combined.release();
        }
    }

    private static byte[] snappyBlockCompress(byte[] data) {
        ByteBuf input = Unpooled.wrappedBuffer(data);
        ByteBuf output = Unpooled.buffer();
        try {
            new Snappy().encode(input, output, data.length);
            byte[] result = new byte[output.readableBytes()];
            output.readBytes(result);
            return result;
        } finally {
            input.release();
            output.release();
        }
    }
}
