/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.DataInput;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadDecoder;

import java.io.IOException;
import java.util.Arrays;

public final class ZstdDecodeStage implements PayloadDecoder {

    private final int blockSize;

    public ZstdDecodeStage(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public byte id() {
        return StageId.ZSTD.id;
    }

    // NOTE: Payload layout: [compressedLen: VInt] [compressed bytes].
    // Decompresses into a native buffer and reads little-endian longs.
    // Expected decompressed size is blockSize x 8 bytes.
    @Override
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        final int uncompressedSize = context.blockSize() * Long.BYTES;
        final int compressedLen = in.readVInt();

        if (uncompressedSize == 0 || compressedLen == 0) {
            Arrays.fill(values, 0, context.blockSize(), 0L);
            return context.blockSize();
        }

        if (context.blockSize() > blockSize) {
            throw new IllegalArgumentException("blockSize " + context.blockSize() + " exceeds stage block size " + blockSize);
        }

        final ZstdBuffers buffers = ZstdBuffers.get(blockSize);
        buffers.src.buffer().clear();
        buffers.dest.buffer().clear();

        for (int read = 0; read < compressedLen;) {
            final int numBytes = Math.min(buffers.copyBuffer.length, compressedLen - read);
            in.readBytes(buffers.copyBuffer, 0, numBytes);
            buffers.src.buffer().put(buffers.copyBuffer, 0, numBytes);
            read += numBytes;
        }
        buffers.src.buffer().flip();

        final int decompressedLen = buffers.zstd.decompress(buffers.dest, buffers.src);
        if (decompressedLen != uncompressedSize) {
            throw new IOException("Expected " + uncompressedSize + " decompressed bytes, got " + decompressedLen);
        }

        final int valueCount = uncompressedSize / Long.BYTES;
        for (int i = 0; i < valueCount; i++) {
            values[i] = buffers.dest.buffer().getLong(i * Long.BYTES);
        }

        return valueCount;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof ZstdDecodeStage that && blockSize == that.blockSize);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(blockSize);
    }

    @Override
    public String toString() {
        return "ZstdDecodeStage{blockSize=" + blockSize + "}";
    }
}
