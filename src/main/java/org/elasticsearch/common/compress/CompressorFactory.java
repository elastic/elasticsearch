/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.google.common.collect.Lists;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.lzf.LZFCompressor;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 */
public class CompressorFactory {

    private static final LZFCompressor LZF = new LZFCompressor();

    private static final Compressor[] compressors;
    private static final ImmutableMap<String, Compressor> compressorsByType;
    private static Compressor defaultCompressor;

    static {
        List<Compressor> compressorsX = Lists.newArrayList();
        compressorsX.add(LZF);

        compressors = compressorsX.toArray(new Compressor[compressorsX.size()]);
        MapBuilder<String, Compressor> compressorsByTypeX = MapBuilder.newMapBuilder();
        for (Compressor compressor : compressors) {
            compressorsByTypeX.put(compressor.type(), compressor);
        }
        compressorsByType = compressorsByTypeX.immutableMap();

        defaultCompressor = LZF;
    }

    public static synchronized void configure(Settings settings) {
        for (Compressor compressor : compressors) {
            compressor.configure(settings);
        }
        String defaultType = settings.get("compress.default.type", "lzf").toLowerCase(Locale.ENGLISH);
        boolean found = false;
        for (Compressor compressor : compressors) {
            if (defaultType.equalsIgnoreCase(compressor.type())) {
                defaultCompressor = compressor;
                found = true;
                break;
            }
        }
        if (!found) {
            Loggers.getLogger(CompressorFactory.class).warn("failed to find default type [{}]", defaultType);
        }
    }

    public static synchronized void setDefaultCompressor(Compressor defaultCompressor) {
        CompressorFactory.defaultCompressor = defaultCompressor;
    }

    public static Compressor defaultCompressor() {
        return defaultCompressor;
    }

    public static boolean isCompressed(BytesReference bytes) {
        return compressor(bytes) != null;
    }

    public static boolean isCompressed(IndexInput in) throws IOException {
        return compressor(in) != null;
    }

    @Nullable
    public static Compressor compressor(BytesReference bytes) {
        for (Compressor compressor : compressors) {
            if (compressor.isCompressed(bytes)) {
                // bytes should be either detected as compressed or as xcontent,
                // if we have bytes that can be either detected as compressed or
                // as a xcontent, we have a problem
                assert XContentFactory.xContentType(bytes) == null;
                return compressor;
            }
        }

        XContentType contentType = XContentFactory.xContentType(bytes);
        if (contentType == null) {
            throw new NotXContentException("Compressor detection can only be called on some xcontent bytes or compressed xcontent bytes");
        }

        return null;
    }

    public static Compressor compressor(ChannelBuffer buffer) {
        for (Compressor compressor : compressors) {
            if (compressor.isCompressed(buffer)) {
                return compressor;
            }
        }
        throw new NotCompressedException();
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
    public static BytesReference uncompressIfNeeded(BytesReference bytes) throws IOException {
        Compressor compressor = compressor(bytes);
        BytesReference uncompressed;
        if (compressor != null) {
            uncompressed = uncompress(bytes, compressor);
        } else {
            uncompressed = bytes;
        }

        return uncompressed;
    }

    /** Decompress the provided {@link BytesReference}. */
    public static BytesReference uncompress(BytesReference bytes) throws IOException {
        Compressor compressor = compressor(bytes);
        if (compressor == null) {
            throw new IllegalArgumentException("Bytes are not compressed");
        }
        return uncompress(bytes, compressor);
    }

    private static BytesReference uncompress(BytesReference bytes, Compressor compressor) throws IOException {
        StreamInput compressed = compressor.streamInput(bytes.streamInput());
        BytesStreamOutput bStream = new BytesStreamOutput();
        Streams.copy(compressed, bStream);
        compressed.close();
        return bStream.bytes();
    }
}
