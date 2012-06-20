/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.compress;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.lzf.LZFCompressor;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

/**
 */
public class CompressorFactory {

    private static final LZFCompressor LZF = new LZFCompressor();

    private static final Compressor[] compressors;
    private static final ImmutableMap<String, Compressor> compressorsByType;

    static {
        compressors = new Compressor[1];
        compressors[0] = LZF;

        MapBuilder<String, Compressor> compressorsByTypeX = MapBuilder.newMapBuilder();
        for (Compressor compressor : compressors) {
            compressorsByTypeX.put(compressor.type(), compressor);
        }
        compressorsByType = compressorsByTypeX.immutableMap();
    }

    public static Compressor defaultCompressor() {
        return LZF;
    }

    public static boolean isCompressed(byte[] data) {
        return compressor(data, 0, data.length) != null;
    }

    public static boolean isCompressed(byte[] data, int offset, int length) {
        return compressor(data, offset, length) != null;
    }

    @Nullable
    public static Compressor compressor(BytesHolder bytes) {
        return compressor(bytes.bytes(), bytes.offset(), bytes.length());
    }

    @Nullable
    public static Compressor compressor(byte[] data) {
        return compressor(data, 0, data.length);
    }

    @Nullable
    public static Compressor compressor(byte[] data, int offset, int length) {
        for (Compressor compressor : compressors) {
            if (compressor.isCompressed(data, offset, length)) {
                return compressor;
            }
        }
        return null;
    }

    @Nullable
    public static Compressor compressor(ChannelBuffer buffer) {
        for (Compressor compressor : compressors) {
            if (compressor.isCompressed(buffer)) {
                return compressor;
            }
        }
        return null;
    }

    @Nullable
    public static Compressor compressor(IndexInput in) throws IOException {
        for (Compressor compressor : compressors) {
            if (compressor.isCompressed(in)) {
                return compressor;
            }
        }
        return null;
    }

    public static Compressor compressor(String type) {
        return compressorsByType.get(type);
    }

    /**
     * Uncompress the provided data, data can be detected as compressed using {@link #isCompressed(byte[], int, int)}.
     */
    public static BytesHolder uncompressIfNeeded(BytesHolder bytes) throws IOException {
        Compressor compressor = compressor(bytes);
        if (compressor != null) {
            return new BytesHolder(compressor.uncompress(bytes.bytes(), bytes.offset(), bytes.length()));
        }
        return bytes;
    }
}
