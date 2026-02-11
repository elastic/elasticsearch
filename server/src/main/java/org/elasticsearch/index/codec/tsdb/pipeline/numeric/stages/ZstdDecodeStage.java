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
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

public final class ZstdDecodeStage implements PayloadDecoder {

    private static final int MAX_COPY_BUFFER_SIZE = 4096;

    private final int blockSize;
    private final CloseableByteBuffer srcBuffer;
    private final CloseableByteBuffer destBuffer;
    private final byte[] copyBuffer;
    private final Zstd zstd;

    public ZstdDecodeStage(int blockSize) {
        this.blockSize = blockSize;
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
    // Decompresses into a native buffer and reads little-endian longs.
    // Expected decompressed size is blockSize × 8 bytes.
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

        srcBuffer.buffer().clear();
        destBuffer.buffer().clear();

        for (int read = 0; read < compressedLen;) {
            final int numBytes = Math.min(copyBuffer.length, compressedLen - read);
            in.readBytes(copyBuffer, 0, numBytes);
            srcBuffer.buffer().put(copyBuffer, 0, numBytes);
            read += numBytes;
        }
        srcBuffer.buffer().flip();

        final int decompressedLen = zstd.decompress(destBuffer, srcBuffer);
        if (decompressedLen != uncompressedSize) {
            throw new IOException("Expected " + uncompressedSize + " decompressed bytes, got " + decompressedLen);
        }

        final int valueCount = uncompressedSize / Long.BYTES;
        for (int i = 0; i < valueCount; i++) {
            values[i] = destBuffer.buffer().getLong(i * Long.BYTES);
        }

        return valueCount;
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
