/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder;
import org.elasticsearch.lz4.ESLZ4Compressor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class Lz4EncodeStage implements PayloadEncoder {

    private final int blockSize;
    private final boolean highCompression;
    private final LZ4Compressor compressor;

    public Lz4EncodeStage(int blockSize, boolean highCompression) {
        this.blockSize = blockSize;
        this.highCompression = highCompression;
        this.compressor = highCompression ? LZ4Factory.safeInstance().highCompressor() : ESLZ4Compressor.INSTANCE;
    }

    public Lz4EncodeStage(int blockSize) {
        this(blockSize, false);
    }

    @Override
    public byte id() {
        return StageId.LZ4.id;
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

        final Lz4Buffers buffers = Lz4Buffers.get(blockSize);
        final int uncompressedSize = valueCount * Long.BYTES;
        writeLongsToBytes(values, valueCount, buffers.src);

        final int compressedLen = compressor.compress(buffers.src, 0, uncompressedSize, buffers.dest, 0, buffers.dest.length);

        out.writeVInt(compressedLen);
        out.writeBytes(buffers.dest, 0, compressedLen);
    }

    private static void writeLongsToBytes(final long[] values, int count, final byte[] dest) {
        for (int i = 0; i < count; i++) {
            final long v = values[i];
            final int off = i * Long.BYTES;
            dest[off] = (byte) v;
            dest[off + 1] = (byte) (v >>> 8);
            dest[off + 2] = (byte) (v >>> 16);
            dest[off + 3] = (byte) (v >>> 24);
            dest[off + 4] = (byte) (v >>> 32);
            dest[off + 5] = (byte) (v >>> 40);
            dest[off + 6] = (byte) (v >>> 48);
            dest[off + 7] = (byte) (v >>> 56);
        }
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof Lz4EncodeStage that && blockSize == that.blockSize && highCompression == that.highCompression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(blockSize, highCompression);
    }

    @Override
    public String toString() {
        return "Lz4EncodeStage{highCompression=" + highCompression + ", blockSize=" + blockSize + "}";
    }
}
