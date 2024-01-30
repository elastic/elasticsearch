/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.zstd.Zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@link org.apache.lucene.codecs.StoredFieldsFormat} that compresses blocks of data using ZStandard.
 *
 * Unlike Lucene's default stored fields format, this format does not make use of dictionaries (even though ZStandard has great support for
 * dictionaries!). This is mostly due to the fact that LZ4/DEFLATE have short sliding windows that they can use to find duplicate strings
 * (64kB and 32kB respectively). In contrast, ZSTD doesn't have such a limitation and can better take advantage of large compression
 * buffers.
 */
public final class Zstd813StoredFieldsFormat extends Lucene90CompressingStoredFieldsFormat {

    public enum Mode {
        BEST_SPEED(0, 16 * 1024, 128),
        BEST_COMPRESSION(9, 256 * 1024, 2048);

        final int level, blockSizeInBytes, blockDocCount;

        private Mode(int level, int blockSizeInBytes, int blockDocCount) {
            this.level = level;
            this.blockSizeInBytes = blockSizeInBytes;
            this.blockDocCount = blockDocCount;
        }
    }

    public Zstd813StoredFieldsFormat(Mode mode) {
        this(mode.level, mode.blockSizeInBytes, mode.blockDocCount);
    }

    Zstd813StoredFieldsFormat(int level, int blockSizeInBytes, int blockDocCount) {
        super("ZstdStoredFields813", new ZstdCompressionMode(level), blockSizeInBytes, blockDocCount, 10);
    }

    private static class ZstdCompressionMode extends CompressionMode {
        private final int level;

        ZstdCompressionMode(int level) {
            this.level = level;
        }

        @Override
        public Compressor newCompressor() {
            return new ZstdCompressor(level);
        }

        @Override
        public Decompressor newDecompressor() {
            return new ZstdDecompressor();
        }

        @Override
        public String toString() {
            return "ZSTD(level=" + level + ")";
        }
    }

    private static final class ZstdDecompressor extends Decompressor {

        byte[] compressed;

        ZstdDecompressor() {
            compressed = BytesRef.EMPTY_BYTES;
        }

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            final int compressedLength = in.readVInt();
            compressed = ArrayUtil.growNoCopy(compressed, compressedLength);
            in.readBytes(compressed, 0, compressedLength);
            bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, originalLength);

            final int l = Zstd.decompress(ByteBuffer.wrap(bytes.bytes), ByteBuffer.wrap(compressed, 0, compressedLength));
            if (l != originalLength) {
                throw new CorruptIndexException("Corrupt", in);
            }
            bytes.offset = offset;
            bytes.length = length;
        }

        @Override
        public Decompressor clone() {
            return new ZstdDecompressor();
        }
    }

    private static class ZstdCompressor extends Compressor {

        final int level;
        byte[] buffer;
        byte[] compressed;

        ZstdCompressor(int level) {
            this.level = level;
            compressed = BytesRef.EMPTY_BYTES;
            buffer = BytesRef.EMPTY_BYTES;
        }

        @Override
        public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
            final int len = Math.toIntExact(buffersInput.length());

            buffer = ArrayUtil.growNoCopy(buffer, len);
            buffersInput.readBytes(buffer, 0, len);

            final int maxCompressedLength = Zstd.getMaxCompressedLen(len);
            compressed = ArrayUtil.growNoCopy(compressed, maxCompressedLength);

            final int compressedLen = Zstd.compress(
                ByteBuffer.wrap(compressed, 0, compressed.length),
                ByteBuffer.wrap(buffer, 0, len),
                level
            );

            out.writeVInt(compressedLen);
            out.writeBytes(compressed, compressedLen);
        }

        @Override
        public void close() throws IOException {}
    }
}
