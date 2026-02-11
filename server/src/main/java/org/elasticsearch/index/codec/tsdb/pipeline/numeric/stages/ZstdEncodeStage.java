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
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

public final class ZstdEncodeStage implements PayloadEncoder {

    public static final int DEFAULT_COMPRESSION_LEVEL = 1;
    private static final int MAX_COPY_BUFFER_SIZE = 4096;

    private final int compressionLevel;
    private final int blockSize;
    private final CloseableByteBuffer srcBuffer;
    private final CloseableByteBuffer destBuffer;
    private final byte[] copyBuffer;
    private final Zstd zstd;

    public ZstdEncodeStage(int blockSize, int compressionLevel) {
        this.blockSize = blockSize;
        this.compressionLevel = compressionLevel;
        final int maxUncompressedSize = blockSize * Long.BYTES;

        final NativeAccess nativeAccess = NativeAccess.instance();
        this.zstd = nativeAccess.getZstd();
        final int compressBound = zstd.compressBound(maxUncompressedSize);

        this.srcBuffer = nativeAccess.newConfinedBuffer(compressBound);
        this.destBuffer = nativeAccess.newConfinedBuffer(compressBound);
        this.copyBuffer = new byte[Math.min(MAX_COPY_BUFFER_SIZE, compressBound)];

        this.srcBuffer.buffer().order(ByteOrder.LITTLE_ENDIAN);
        this.destBuffer.buffer().order(ByteOrder.LITTLE_ENDIAN);
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

        final int uncompressedSize = valueCount * Long.BYTES;
        srcBuffer.buffer().clear();
        destBuffer.buffer().clear();

        for (int i = 0; i < valueCount; i++) {
            srcBuffer.buffer().putLong(values[i]);
        }
        srcBuffer.buffer().flip();

        final int compressedLen = zstd.compress(destBuffer, srcBuffer, compressionLevel);

        out.writeVInt(compressedLen);
        for (int written = 0; written < compressedLen;) {
            final int numBytes = Math.min(copyBuffer.length, compressedLen - written);
            destBuffer.buffer().get(copyBuffer, 0, numBytes);
            out.writeBytes(copyBuffer, 0, numBytes);
            written += numBytes;
        }
    }

    // NOTE: srcBuffer and destBuffer are native (off-heap) allocations via
    // NativeAccess. The JVM GC does not track or reclaim native memory, so
    // failing to close these buffers leaks memory outside the Java heap.
    // requiresExplicitClose() signals the pipeline to call close() when the
    // stage is no longer needed, rather than relying on GC finalization.
    @Override
    public boolean requiresExplicitClose() {
        return true;
    }

    @Override
    public void close() {
        srcBuffer.close();
        destBuffer.close();
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
