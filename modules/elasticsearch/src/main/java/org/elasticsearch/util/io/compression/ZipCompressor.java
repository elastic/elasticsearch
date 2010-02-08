/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.util.io.compression;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.io.FastByteArrayOutputStream;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * @author kimchy (Shay Banon)
 */
public class ZipCompressor implements Compressor {

    private static class Cached {

        private static final ThreadLocal<CompressHolder> cache = new ThreadLocal<CompressHolder>() {
            @Override protected CompressHolder initialValue() {
                return new CompressHolder();
            }
        };

        /**
         * Returns the cached thread local byte strean, with its internal stream cleared.
         */
        public static CompressHolder cached() {
            CompressHolder ch = cache.get();
            ch.bos.reset();
            return ch;
        }
    }


    private static class CompressHolder {
        final FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        final Deflater deflater = new Deflater();
        final Inflater inflater = new Inflater();
        final byte[] buffer = new byte[(int) SizeUnit.KB.toBytes(5)];
        final UnicodeUtil.UTF16Result utf16Result = new UnicodeUtil.UTF16Result();
        final UnicodeUtil.UTF8Result utf8Result = new UnicodeUtil.UTF8Result();
    }

    private final int compressionLevel;

    public ZipCompressor() {
        this(Deflater.BEST_COMPRESSION);
    }

    public ZipCompressor(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    public byte[] compress(byte[] value, int offset, int length) throws IOException {
        return compress(value, offset, length, compressionLevel, Cached.cached());
    }

    @Override public byte[] compress(byte[] value) throws IOException {
        return compress(value, 0, value.length);
    }

    @Override public byte[] compressString(String value) throws IOException {
        CompressHolder ch = Cached.cached();
        UnicodeUtil.UTF16toUTF8(value, 0, value.length(), ch.utf8Result);
        return compress(ch.utf8Result.result, 0, ch.utf8Result.length, compressionLevel, ch);
    }

    @Override public byte[] decompress(byte[] value) throws IOException {
        CompressHolder ch = Cached.cached();
        decompress(value, ch);
        return ch.bos.copiedByteArray();
    }

    @Override public String decompressString(byte[] value) throws IOException {
        CompressHolder ch = Cached.cached();
        decompress(value, ch);
        UnicodeUtil.UTF8toUTF16(ch.bos.unsafeByteArray(), 0, ch.bos.size(), ch.utf16Result);
        return new String(ch.utf16Result.result, 0, ch.utf16Result.length);
    }

    private static void decompress(byte[] value, CompressHolder ch) throws IOException {
        try {
            ch.inflater.reset();
            ch.inflater.setInput(value);

            // Decompress the data
            final byte[] buf = ch.buffer;
            while (!ch.inflater.finished()) {
                int count = ch.inflater.inflate(buf);
                ch.bos.write(buf, 0, count);
            }
        } catch (DataFormatException e) {
            throw new IOException("Failed to decompress", e);
        } // don't close the inflater, we reuse it...
    }

    private static byte[] compress(byte[] value, int offset, int length, int compressionLevel, CompressHolder ch) throws IOException {
        ch.deflater.reset();
        ch.deflater.setLevel(compressionLevel);
        ch.deflater.setInput(value, offset, length);
        ch.deflater.finish();

        // Compress the data
        final byte[] buf = ch.buffer;
        while (!ch.deflater.finished()) {
            int count = ch.deflater.deflate(buf);
            ch.bos.write(buf, 0, count);
        }

        return ch.bos.copiedByteArray();
    }
}
