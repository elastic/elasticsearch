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
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadCodecStage;
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

public final class ZstdCodecStage implements PayloadCodecStage {

    public static final int DEFAULT_COMPRESSION_LEVEL = 1;
    private static final int MAX_COPY_BUFFER_SIZE = 4096;

    private final int compressionLevel;
    private final int blockSize;

    private final CloseableByteBuffer srcBuffer;
    private final CloseableByteBuffer destBuffer;
    private final byte[] copyBuffer;
    private final Zstd zstd;

    public ZstdCodecStage(int blockSize, int compressionLevel) {
        this.blockSize = blockSize;
        this.compressionLevel = compressionLevel;
        int maxUncompressedSize = blockSize * Long.BYTES;

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

    @Override
    public String name() {
        return "zstd";
    }

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

    public int blockSize() {
        return blockSize;
    }

    @Override
    public void close() {
        srcBuffer.close();
        destBuffer.close();
    }
}
