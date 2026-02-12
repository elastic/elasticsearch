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
import org.elasticsearch.lz4.ESLZ4Decompressor;

import java.io.IOException;
import java.util.Objects;

public final class Lz4DecodeStage implements PayloadDecoder {

    private final int blockSize;

    public Lz4DecodeStage(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public byte id() {
        return StageId.LZ4.id;
    }

    @Override
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        final int compressedLen = in.readVInt();
        if (compressedLen == 0) {
            return 0;
        }

        final Lz4Buffers buffers = Lz4Buffers.get(blockSize);
        in.readBytes(buffers.dest, 0, compressedLen);

        final int uncompressedSize = blockSize * Long.BYTES;
        ESLZ4Decompressor.INSTANCE.decompress(buffers.dest, 0, buffers.src, 0, uncompressedSize);

        readBytesToLongs(buffers.src, values, blockSize);
        return blockSize;
    }

    private static void readBytesToLongs(final byte[] src, final long[] values, int count) {
        for (int i = 0; i < count; i++) {
            final int off = i * Long.BYTES;
            values[i] = (src[off] & 0xFFL) | ((src[off + 1] & 0xFFL) << 8) | ((src[off + 2] & 0xFFL) << 16) | ((src[off + 3] & 0xFFL) << 24)
                | ((src[off + 4] & 0xFFL) << 32) | ((src[off + 5] & 0xFFL) << 40) | ((src[off + 6] & 0xFFL) << 48) | ((src[off + 7] & 0xFFL)
                    << 56);
        }
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof Lz4DecodeStage that && blockSize == that.blockSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(blockSize);
    }

    @Override
    public String toString() {
        return "Lz4DecodeStage{blockSize=" + blockSize + "}";
    }
}
