/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.compress.LZ4;

import java.io.IOException;

public class LZ4CompressionMode extends CompressionMode {

    public static final LZ4CompressionMode INSTANCE = new LZ4CompressionMode();

    private LZ4CompressionMode() {}

    @Override
    public Compressor newCompressor() {
        return new LZ4Compressor();
    }

    @Override
    public Decompressor newDecompressor() {
        return new LZ4Decompressor();
    }

    @Override
    public String toString() {
        return "LZ4";
    }

    private static final class LZ4Compressor extends Compressor {

        private final LZ4.FastCompressionHashTable hashTable = new LZ4.FastCompressionHashTable();
        private byte[] buffer = BytesRef.EMPTY_BYTES;

        private LZ4Compressor() {}

        @Override
        public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
            final int srcLen = Math.toIntExact(buffersInput.length());
            if (srcLen == 0) {
                return;
            }

            buffer = ArrayUtil.growNoCopy(buffer, srcLen);
            buffersInput.readBytes(buffer, 0, srcLen);

            out.writeVInt(srcLen);
            LZ4.compress(buffer, 0, srcLen, out, hashTable);
        }

        @Override
        public void close() throws IOException {}
    }

    private static final class LZ4Decompressor extends Decompressor {

        private byte[] buffer = BytesRef.EMPTY_BYTES;

        private LZ4Decompressor() {}

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            if (originalLength == 0) {
                bytes.offset = 0;
                bytes.length = 0;
                return;
            }

            final int uncompressedLength = in.readVInt();

            buffer = ArrayUtil.growNoCopy(buffer, uncompressedLength);
            LZ4.decompress(in, uncompressedLength, buffer, 0);

            bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, length);
            System.arraycopy(buffer, offset, bytes.bytes, 0, length);
            bytes.offset = 0;
            bytes.length = length;
        }

        @Override
        public Decompressor clone() {
            return new LZ4Decompressor();
        }
    }
}
