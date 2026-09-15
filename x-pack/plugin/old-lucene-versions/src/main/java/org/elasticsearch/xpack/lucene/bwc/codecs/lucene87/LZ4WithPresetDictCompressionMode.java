/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene87;

import org.apache.lucene.backward_codecs.compressing.CompressionMode;
import org.apache.lucene.backward_codecs.compressing.Compressor;
import org.apache.lucene.backward_codecs.compressing.Decompressor;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.compress.LZ4;

import java.io.IOException;

/**
 * A compression mode that compromises on the compression ratio to provide fast compression and
 * decompression.
 *
 * @lucene.internal
 */
public final class LZ4WithPresetDictCompressionMode extends CompressionMode {

    // Shoot for 10 sub blocks
    private static final int NUM_SUB_BLOCKS = 10;
    // And a dictionary whose size is about 2x smaller than sub blocks
    private static final int DICT_SIZE_FACTOR = 2;

    /** Sole constructor. */
    public LZ4WithPresetDictCompressionMode() {}

    @Override
    public Compressor newCompressor() {
        return new LZ4WithPresetDictCompressor();
    }

    @Override
    public Decompressor newDecompressor() {
        return new LZ4WithPresetDictDecompressor();
    }

    @Override
    public String toString() {
        return "BEST_SPEED";
    }

    private static final class LZ4WithPresetDictDecompressor extends Decompressor {

        private int[] compressedLengths;
        private byte[] buffer;

        LZ4WithPresetDictDecompressor() {
            compressedLengths = new int[0];
            buffer = new byte[0];
        }

        private int readCompressedLengths(DataInput in, int originalLength, int dictLength, int blockLength) throws IOException {
            in.readVInt(); // compressed length of the dictionary, unused
            int totalLength = dictLength;
            int i = 0;
            while (totalLength < originalLength) {
                compressedLengths = ArrayUtil.grow(compressedLengths, i + 1);
                compressedLengths[i++] = in.readVInt();
                totalLength += blockLength;
            }
            return i;
        }

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            assert offset + length <= originalLength;

            if (length == 0) {
                bytes.length = 0;
                return;
            }

            final int dictLength = in.readVInt();
            final int blockLength = in.readVInt();

            final int numBlocks = readCompressedLengths(in, originalLength, dictLength, blockLength);

            buffer = ArrayUtil.grow(buffer, dictLength + blockLength);
            bytes.length = 0;
            // Read the dictionary
            if (LZ4.decompress(EndiannessReverserUtil.wrapDataInput(in), dictLength, buffer, 0) != dictLength) {
                throw new CorruptIndexException("Illegal dict length", in);
            }

            int offsetInBlock = dictLength;
            int offsetInBytesRef = offset;
            if (offset >= dictLength) {
                offsetInBytesRef -= dictLength;

                // Skip unneeded blocks
                int numBytesToSkip = 0;
                for (int i = 0; i < numBlocks && offsetInBlock + blockLength < offset; ++i) {
                    int compressedBlockLength = compressedLengths[i];
                    numBytesToSkip += compressedBlockLength;
                    offsetInBlock += blockLength;
                    offsetInBytesRef -= blockLength;
                }
                in.skipBytes(numBytesToSkip);
            } else {
                // The dictionary contains some bytes we need, copy its content to the BytesRef
                bytes.bytes = ArrayUtil.grow(bytes.bytes, dictLength);
                System.arraycopy(buffer, 0, bytes.bytes, 0, dictLength);
                bytes.length = dictLength;
            }

            // Read blocks that intersect with the interval we need
            while (offsetInBlock < offset + length) {
                final int bytesToDecompress = Math.min(blockLength, offset + length - offsetInBlock);
                LZ4.decompress(EndiannessReverserUtil.wrapDataInput(in), bytesToDecompress, buffer, dictLength);
                bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + bytesToDecompress);
                System.arraycopy(buffer, dictLength, bytes.bytes, bytes.length, bytesToDecompress);
                bytes.length += bytesToDecompress;
                offsetInBlock += blockLength;
            }

            bytes.offset = offsetInBytesRef;
            bytes.length = length;
            assert bytes.isValid();
        }

        @Override
        public Decompressor clone() {
            return new LZ4WithPresetDictDecompressor();
        }
    }

    private static class LZ4WithPresetDictCompressor extends Compressor {

        final ByteBuffersDataOutput compressed;
        final LZ4.FastCompressionHashTable hashTable;
        byte[] buffer;

        LZ4WithPresetDictCompressor() {
            compressed = ByteBuffersDataOutput.newResettableInstance();
            hashTable = new LZ4.FastCompressionHashTable();
            buffer = BytesRef.EMPTY_BYTES;
        }

        private void doCompress(byte[] bytes, int dictLen, int len, DataOutput out) throws IOException {
            long prevCompressedSize = compressed.size();
            LZ4.compressWithDictionary(bytes, 0, dictLen, len, compressed, hashTable);
            // Write the number of compressed bytes
            out.writeVInt(Math.toIntExact(compressed.size() - prevCompressedSize));
        }

        @Override
        public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
            final int dictLength = len / (NUM_SUB_BLOCKS * DICT_SIZE_FACTOR);
            final int blockLength = (len - dictLength + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            buffer = ArrayUtil.grow(buffer, dictLength + blockLength);
            out.writeVInt(dictLength);
            out.writeVInt(blockLength);
            final int end = off + len;

            compressed.reset();
            // Compress the dictionary first
            System.arraycopy(bytes, off, buffer, 0, dictLength);
            doCompress(buffer, 0, dictLength, out);

            // And then sub blocks
            for (int start = off + dictLength; start < end; start += blockLength) {
                int l = Math.min(blockLength, off + len - start);
                System.arraycopy(bytes, start, buffer, dictLength, l);
                doCompress(buffer, dictLength, l, out);
            }

            // We only wrote lengths so far, now write compressed data
            compressed.copyTo(out);
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }
}
