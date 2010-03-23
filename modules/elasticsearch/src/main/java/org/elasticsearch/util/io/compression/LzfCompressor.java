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
import org.elasticsearch.util.ThreadLocals;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.compression.lzf.LZFDecoder;
import org.elasticsearch.util.io.compression.lzf.LZFEncoder;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class LzfCompressor implements Compressor {

    private static class Cached {

        private static final ThreadLocal<ThreadLocals.CleanableValue<CompressHolder>> cache = new ThreadLocal<ThreadLocals.CleanableValue<CompressHolder>>() {
            @Override protected ThreadLocals.CleanableValue<CompressHolder> initialValue() {
                return new ThreadLocals.CleanableValue<CompressHolder>(new CompressHolder());
            }
        };

        public static CompressHolder cached() {
            return cache.get().get();
        }
    }

    private static class CompressHolder {
        final UnicodeUtil.UTF8Result utf8Result = new UnicodeUtil.UTF8Result();
    }

    @Override public byte[] compress(byte[] value) throws IOException {
        return LZFEncoder.encode(value, value.length);
    }

    @Override public byte[] compressString(String value) throws IOException {
        CompressHolder ch = Cached.cached();
        UnicodeUtil.UTF16toUTF8(value, 0, value.length(), ch.utf8Result);
        return LZFEncoder.encode(ch.utf8Result.result, ch.utf8Result.length);
    }

    @Override public byte[] decompress(byte[] value) throws IOException {
        return LZFDecoder.decode(value, value.length);
    }

    @Override public String decompressString(byte[] value) throws IOException {
        CompressHolder ch = Cached.cached();
        byte[] result = decompress(value);
        return Unicode.fromBytes(result, 0, result.length);
    }
}
