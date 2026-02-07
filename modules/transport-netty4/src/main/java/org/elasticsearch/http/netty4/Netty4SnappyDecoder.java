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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.compression.Snappy;
import io.netty.handler.codec.compression.SnappyFrameDecoder;

import java.util.List;

/**
 * A snappy decoder that auto-detects whether the content uses snappy <em>block</em> format
 * or snappy <em>framed</em> (streaming) format, and decompresses accordingly.
 * <p>
 * Netty's built-in {@link SnappyFrameDecoder} only handles the framed format, but some
 * protocols (notably <a href="https://prometheus.io/docs/concepts/remote_write_spec/">
 * Prometheus remote write</a>) mandate the block format. This decoder transparently
 * supports both so that {@code Content-Encoding: snappy} works regardless of which
 * format the client uses.
 * <p>
 * Detection is based on the first bytes of the payload. The snappy framed format always
 * begins with a 10-byte stream identifier: {@code ff 06 00 00 73 4e 61 50 70 59}
 * (chunk type {@code 0xff}, chunk length {@code 6}, followed by "sNaPpY"). If the
 * payload starts with these exact bytes, framed format is assumed; otherwise block
 * format is used.
 */
class Netty4SnappyDecoder extends SnappyFrameDecoder {

    /**
     * The snappy framed format stream identifier: chunk type 0xff, chunk data length 6 (little-endian),
     * followed by the ASCII bytes "sNaPpY".
     */
    static final byte[] FRAMED_MAGIC = new byte[] { (byte) 0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59 };

    private Boolean isBlockFormat;
    private Snappy blockSnappy;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (isBlockFormat == null) {
            isBlockFormat = detectBlockFormat(in);
            if (isBlockFormat == null) {
                return; // need more bytes to decide
            }
            if (isBlockFormat) {
                blockSnappy = new Snappy();
            }
        }

        if (isBlockFormat) {
            decodeBlock(ctx, in, out);
        } else {
            super.decode(ctx, in, out);
        }
    }

    @Override
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (isBlockFormat == null && in.isReadable()) {
            // We never accumulated enough bytes for the full magic check, but a valid framed
            // stream always starts with the 10-byte identifier, so this must be block format.
            isBlockFormat = true;
            blockSnappy = new Snappy();
        }
        super.decodeLast(ctx, in, out);
    }

    /**
     * Determines the snappy format from the buffered bytes.
     *
     * @return {@code true} for block format, {@code false} for framed format,
     *         or {@code null} if more bytes are needed to decide.
     */
    private static Boolean detectBlockFormat(ByteBuf in) {
        if (in.isReadable() == false) {
            return null;
        }
        // Quick check: if the first byte is not 0xff it cannot be framed format.
        if (in.getUnsignedByte(in.readerIndex()) != 0xff) {
            return true;
        }
        // First byte is 0xff -- could be either format. We need the full 10-byte
        // stream identifier to be sure.
        if (in.readableBytes() < FRAMED_MAGIC.length) {
            return null; // wait for more data
        }
        for (int i = 0; i < FRAMED_MAGIC.length; i++) {
            if (in.getByte(in.readerIndex() + i) != FRAMED_MAGIC[i]) {
                return true; // mismatch → block format
            }
        }
        return false; // full magic matches → framed format
    }

    private void decodeBlock(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.isReadable()) {
            ByteBuf uncompressed = ctx.alloc().buffer();
            try {
                blockSnappy.decode(in, uncompressed);
                if (uncompressed.isReadable()) {
                    out.add(uncompressed);
                    uncompressed = null;
                }
            } finally {
                if (uncompressed != null) {
                    uncompressed.release();
                }
            }
        }
    }
}
