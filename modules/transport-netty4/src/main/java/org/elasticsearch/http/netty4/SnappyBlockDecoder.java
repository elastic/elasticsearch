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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.Snappy;

import java.util.List;

/**
 * A Netty {@link ByteToMessageDecoder} that decodes Snappy block format compressed data.
 */
final class SnappyBlockDecoder extends ByteToMessageDecoder {
    private static final int INCOMPLETE_PREAMBLE = -1;

    private final Snappy snappy = new Snappy();
    private final int maxUncompressedSize;
    private int expectedUncompressedSize = INCOMPLETE_PREAMBLE;
    private ByteBuf decompressed;
    private boolean sawInput;
    private boolean finished;
    private boolean corrupted;

    SnappyBlockDecoder(int maxUncompressedSize) {
        this.maxUncompressedSize = maxUncompressedSize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (corrupted) {
            in.skipBytes(in.readableBytes());
            return;
        }

        try {
            decodeInternal(ctx, in, out);
        } catch (Exception e) {
            corrupted = true;
            releaseDecompressedBuffer();
            throw e;
        }
    }

    @Override
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (corrupted) {
            in.skipBytes(in.readableBytes());
            return;
        }

        decode(ctx, in, out);
        if (finished || sawInput == false) {
            return;
        }

        corrupted = true;
        releaseDecompressedBuffer();
        if (expectedUncompressedSize == INCOMPLETE_PREAMBLE) {
            throw new DecompressionException("truncated snappy preamble");
        }

        int produced = decompressed == null ? 0 : decompressed.writerIndex();
        throw new DecompressionException(
            "truncated snappy block: expected [" + expectedUncompressedSize + "] bytes but decoded [" + produced + "]"
        );
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) {
        releaseDecompressedBuffer();
        snappy.reset();
    }

    private void decodeInternal(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (finished) {
            if (in.isReadable()) {
                throw new DecompressionException("snappy block has trailing bytes after decompression completed");
            }
            return;
        }

        if (in.isReadable() == false) {
            return;
        }

        sawInput = true;

        if (expectedUncompressedSize == INCOMPLETE_PREAMBLE) {
            int uncompressedSize = readUncompressedLength(in);
            if (uncompressedSize == INCOMPLETE_PREAMBLE) {
                return;
            }
            if (uncompressedSize > maxUncompressedSize) {
                throw new DecompressionException(
                    "snappy uncompressed size [" + uncompressedSize + "] exceeds maximum allowed size [" + maxUncompressedSize + "]"
                );
            }
            expectedUncompressedSize = uncompressedSize;
            decompressed = ctx.alloc().buffer(expectedUncompressedSize, expectedUncompressedSize);
        }

        snappy.decode(in, decompressed);
        int produced = decompressed.writerIndex();

        if (produced == expectedUncompressedSize) {
            if (in.isReadable()) {
                throw new DecompressionException("snappy block has trailing bytes after decompression completed");
            }
            finish(out);
        }
    }

    private void finish(List<Object> out) {
        ByteBuf output = decompressed;
        decompressed = null;
        finished = true;
        snappy.reset();
        if (output.isReadable()) {
            out.add(output);
        } else {
            output.release();
        }
    }

    private void releaseDecompressedBuffer() {
        if (decompressed != null) {
            decompressed.release();
            decompressed = null;
        }
    }

    private static int readUncompressedLength(ByteBuf in) {
        in.markReaderIndex();
        try {
            return readPreamble(in);
        } finally {
            in.resetReaderIndex();
        }
    }

    private static int readPreamble(ByteBuf in) {
        int length = 0;
        int byteIndex = 0;
        while (in.isReadable()) {
            int current = in.readUnsignedByte();
            length |= (current & 0x7f) << byteIndex++ * 7;
            if ((current & 0x80) == 0) {
                return length;
            }

            if (byteIndex >= 4) {
                throw new DecompressionException("Preamble is greater than 4 bytes");
            }
        }

        return INCOMPLETE_PREAMBLE;
    }
}
