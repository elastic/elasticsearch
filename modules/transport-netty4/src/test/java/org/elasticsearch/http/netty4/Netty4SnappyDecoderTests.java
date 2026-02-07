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
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Snappy;
import io.netty.handler.codec.compression.SnappyFrameEncoder;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class Netty4SnappyDecoderTests extends ESTestCase {

    public void testBlockFormatDecompression() {
        byte[] original = "Hello, snappy block format!".getBytes(StandardCharsets.UTF_8);

        ByteBuf blockCompressed = compressBlock(original);
        // Confirm that the compressed block format does NOT start with the framed format's magic bytes
        assertThat(blockCompressed.getUnsignedByte(0), not(equalTo((short) 0xff)));

        assertDecompresses(blockCompressed, original);
    }

    public void testFramedFormatDecompression() {
        byte[] original = "Hello, snappy framed format!".getBytes(StandardCharsets.UTF_8);

        ByteBuf framedCompressed = compressFramed(original);

        // Confirm we actually have framed format (starts with 0xff stream identifier)
        assertThat(framedCompressed.getUnsignedByte(0), equalTo((short) 0xff));

        assertDecompresses(framedCompressed, original);
    }

    public void testBlockFormatLargePayload() {
        byte[] original = new byte[10_000];
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) (i % 256);
        }
        assertBlockRoundTrip(original);
    }

    public void testBlockFormatWithVarintStarting0xff() {
        // A payload whose uncompressed length % 128 == 127 produces a varint starting with 0xff.
        // This must be detected as block format, not framed.
        // Length 255 -> varint bytes: 0xff 0x01
        byte[] original = new byte[255];
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) i;
        }

        ByteBuf compressed = compressBlock(original);

        // Verify the compressed payload actually starts with 0xff (the varint for length 255)
        assertThat(
            "expected block-format varint to start with 0xff for a 255-byte payload",
            compressed.getUnsignedByte(compressed.readerIndex()),
            equalTo((short) 0xff)
        );

        assertDecompresses(compressed, original);
    }

    public void testBlockFormatWithVarintStarting0xffLarger() {
        // Length 895 -> varint bytes: 0xff 0x06. The second byte (0x06) matches the framed
        // format's chunk length byte, so this exercises the deeper magic check.
        byte[] original = new byte[895];
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) i;
        }

        ByteBuf compressed = compressBlock(original);
        assertThat(
            "expected block-format varint to start with 0xff for an 895-byte payload",
            compressed.getUnsignedByte(compressed.readerIndex()),
            equalTo((short) 0xff)
        );

        assertDecompresses(compressed, original);
    }

    public void testBlockFormatChunkedDeliveryWithVarintStarting0xff() {
        // When the block-format varint starts with 0xff, the decoder needs up to 10 bytes
        // to distinguish it from the framed format's stream identifier. This test feeds the
        // compressed payload in two chunks to exercise the detectBlockFormat → null path.
        byte[] original = new byte[255];
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) i;
        }

        ByteBuf compressed = compressBlock(original);
        assertThat(
            "expected block-format varint to start with 0xff for a 255-byte payload",
            compressed.getUnsignedByte(compressed.readerIndex()),
            equalTo((short) 0xff)
        );

        // Split into two chunks: first chunk is shorter than the 10-byte magic
        int split = randomIntBetween(1, Math.min(9, compressed.readableBytes() - 1));
        ByteBuf firstChunk = compressed.readRetainedSlice(split);
        ByteBuf secondChunk = compressed; // remainder

        EmbeddedChannel channel = new EmbeddedChannel(new Netty4SnappyDecoder());
        try {
            channel.writeInbound(firstChunk);
            // No output yet — decoder is still waiting for enough bytes to decide the format
            assertNull(channel.readInbound());

            channel.writeInbound(secondChunk);
            channel.finish();

            ByteBuf decompressed = channel.readInbound();
            try {
                byte[] result = new byte[decompressed.readableBytes()];
                decompressed.readBytes(result);
                assertThat(result, equalTo(original));
            } finally {
                decompressed.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    // --- helpers ---

    private static ByteBuf compressBlock(byte[] data) {
        ByteBuf compressed = Unpooled.buffer();
        new Snappy().encode(Unpooled.wrappedBuffer(data), compressed, data.length);
        return compressed;
    }

    private static ByteBuf compressFramed(byte[] data) {
        EmbeddedChannel encoder = new EmbeddedChannel(new SnappyFrameEncoder());
        encoder.writeOutbound(Unpooled.wrappedBuffer(data));
        encoder.finish();
        ByteBuf result = Unpooled.buffer();
        ByteBuf chunk;
        while ((chunk = encoder.readOutbound()) != null) {
            result.writeBytes(chunk);
            chunk.release();
        }
        encoder.finishAndReleaseAll();
        return result;
    }

    private static void assertBlockRoundTrip(byte[] original) {
        assertDecompresses(compressBlock(original), original);
    }

    private static void assertDecompresses(ByteBuf compressed, byte[] expected) {
        EmbeddedChannel channel = new EmbeddedChannel(new Netty4SnappyDecoder());
        try {
            channel.writeInbound(compressed);
            channel.finish();

            ByteBuf decompressed = channel.readInbound();
            try {
                byte[] result = new byte[decompressed.readableBytes()];
                decompressed.readBytes(result);
                assertThat(result, equalTo(expected));
            } finally {
                decompressed.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
