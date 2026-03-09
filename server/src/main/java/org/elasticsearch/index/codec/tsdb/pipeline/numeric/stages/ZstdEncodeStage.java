/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class ZstdEncodeStage implements PayloadEncoder {

    public static final int DEFAULT_COMPRESSION_LEVEL = 1;

    private final int compressionLevel;
    private final int blockSize;

    public ZstdEncodeStage(int blockSize, int compressionLevel) {
        this.blockSize = blockSize;
        this.compressionLevel = compressionLevel;
    }

    @Override
    public byte id() {
        return StageId.ZSTD.id;
    }

    // NOTE: Payload layout: [compressedLen: VInt] [compressed bytes].
    // Values are written as little-endian longs into a native buffer, then
    // Zstd-compressed. The block is always padded to blockSize before compression.
    @Override
    public void encode(final long[] values, int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        if (valueCount == 0) {
            out.writeVInt(0);
            return;
        }

        if (valueCount > blockSize) {
            throw new IllegalArgumentException("valueCount " + valueCount + " exceeds block size " + blockSize);
        }

        if (valueCount < blockSize) {
            Arrays.fill(values, valueCount, blockSize, 0L);
            valueCount = blockSize;
        }

        final ZstdBuffers buffers = ZstdBuffers.get(blockSize);
        buffers.src.buffer().clear();
        buffers.dest.buffer().clear();

        for (int i = 0; i < valueCount; i++) {
            buffers.src.buffer().putLong(values[i]);
        }
        buffers.src.buffer().flip();

        final int compressedLen = buffers.zstd.compress(buffers.dest, buffers.src, compressionLevel);

        out.writeVInt(compressedLen);
        for (int written = 0; written < compressedLen;) {
            final int numBytes = Math.min(buffers.copyBuffer.length, compressedLen - written);
            buffers.dest.buffer().get(buffers.copyBuffer, 0, numBytes);
            out.writeBytes(buffers.copyBuffer, 0, numBytes);
            written += numBytes;
        }
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof ZstdEncodeStage that && compressionLevel == that.compressionLevel && blockSize == that.blockSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compressionLevel, blockSize);
    }

    @Override
    public String toString() {
        return "ZstdEncodeStage{compressionLevel=" + compressionLevel + ", blockSize=" + blockSize + "}";
    }
}
