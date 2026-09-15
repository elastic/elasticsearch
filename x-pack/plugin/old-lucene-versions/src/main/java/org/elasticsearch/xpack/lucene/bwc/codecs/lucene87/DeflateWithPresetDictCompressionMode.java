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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * A compression mode that trades speed for compression ratio. Although compression and
 * decompression might be slow, this compression mode should provide a good compression ratio. This
 * mode might be interesting if/when your index size is much bigger than your OS cache.
 *
 * @lucene.internal
 */
public final class DeflateWithPresetDictCompressionMode extends CompressionMode {

    // Shoot for 10 sub blocks
    private static final int NUM_SUB_BLOCKS = 10;
    // And a dictionary whose size is about 6x smaller than sub blocks
    private static final int DICT_SIZE_FACTOR = 6;

    /** Sole constructor. */
    public DeflateWithPresetDictCompressionMode() {}

    @Override
    public Compressor newCompressor() {
        // notes:
        // 3 is the highest level that doesn't have lazy match evaluation
        // 6 is the default, higher than that is just a waste of cpu
        return new DeflateWithPresetDictCompressor(6);
    }

    @Override
    public Decompressor newDecompressor() {
        return new DeflateWithPresetDictDecompressor();
    }

    @Override
    public String toString() {
        return "BEST_COMPRESSION";
    }

    private static final class DeflateWithPresetDictDecompressor extends Decompressor {

        byte[] compressed;

        DeflateWithPresetDictDecompressor() {
            compressed = new byte[0];
        }

        private void doDecompress(DataInput in, Inflater decompressor, BytesRef bytes) throws IOException {
            final int compressedLength = in.readVInt();
            if (compressedLength == 0) {
                return;
            }
            // pad with extra "dummy byte": see javadocs for using Inflater(true)
            // we do it for compliance, but it's unnecessary for years in zlib.
            final int paddedLength = compressedLength + 1;
            compressed = ArrayUtil.grow(compressed, paddedLength);
            in.readBytes(compressed, 0, compressedLength);
            compressed[compressedLength] = 0; // explicitly set dummy byte to 0

            // extra "dummy byte"
            decompressor.setInput(compressed, 0, paddedLength);
            try {
                bytes.length += decompressor.inflate(bytes.bytes, bytes.length, bytes.bytes.length - bytes.length);
            } catch (DataFormatException e) {
                throw new IOException(e);
            }
            if (decompressor.finished() == false) {
                throw new CorruptIndexException(
                    "Invalid decoder state: needsInput=" + decompressor.needsInput() + ", needsDict=" + decompressor.needsDictionary(),
                    in
                );
            }
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
            bytes.bytes = ArrayUtil.grow(bytes.bytes, dictLength);
            bytes.offset = bytes.length = 0;

            final Inflater decompressor = new Inflater(true);
            try {
                // Read the dictionary
                doDecompress(in, decompressor, bytes);
                if (dictLength != bytes.length) {
                    throw new CorruptIndexException("Unexpected dict length", in);
                }

                int offsetInBlock = dictLength;
                int offsetInBytesRef = offset;

                // Skip unneeded blocks
                while (offsetInBlock + blockLength < offset) {
                    final int compressedLength = in.readVInt();
                    in.skipBytes(compressedLength);
                    offsetInBlock += blockLength;
                    offsetInBytesRef -= blockLength;
                }

                // Read blocks that intersect with the interval we need
                while (offsetInBlock < offset + length) {
                    bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + blockLength);
                    decompressor.reset();
                    decompressor.setDictionary(bytes.bytes, 0, dictLength);
                    doDecompress(in, decompressor, bytes);
                    offsetInBlock += blockLength;
                }

                bytes.offset = offsetInBytesRef;
                bytes.length = length;
                assert bytes.isValid();
            } finally {
                decompressor.end();
            }
        }

        @Override
        public Decompressor clone() {
            return new DeflateWithPresetDictDecompressor();
        }
    }

    private static class DeflateWithPresetDictCompressor extends Compressor {

        final Deflater compressor;
        byte[] compressed;
        boolean closed;

        DeflateWithPresetDictCompressor(int level) {
            compressor = new Deflater(level, true);
            compressed = new byte[64];
        }

        private void doCompress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
            if (len == 0) {
                out.writeVInt(0);
                return;
            }
            compressor.setInput(bytes, off, len);
            compressor.finish();
            if (compressor.needsInput()) {
                throw new IllegalStateException();
            }

            int totalCount = 0;
            for (;;) {
                final int count = compressor.deflate(compressed, totalCount, compressed.length - totalCount);
                totalCount += count;
                assert totalCount <= compressed.length;
                if (compressor.finished()) {
                    break;
                } else {
                    compressed = ArrayUtil.grow(compressed);
                }
            }

            out.writeVInt(totalCount);
            out.writeBytes(compressed, totalCount);
        }

        @Override
        public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
            final int dictLength = len / (NUM_SUB_BLOCKS * DICT_SIZE_FACTOR);
            final int blockLength = (len - dictLength + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            out.writeVInt(dictLength);
            out.writeVInt(blockLength);
            final int end = off + len;

            // Compress the dictionary first
            compressor.reset();
            doCompress(bytes, off, dictLength, out);

            // And then sub blocks
            for (int start = off + dictLength; start < end; start += blockLength) {
                compressor.reset();
                compressor.setDictionary(bytes, off, dictLength);
                doCompress(bytes, start, Math.min(blockLength, off + len - start), out);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed == false) {
                compressor.end();
                closed = true;
            }
        }
    }
}
