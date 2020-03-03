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

package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Collection of helper methods. Similar to CompressedXContent, but this utilizes GZIP.
 */
public final class InferenceToXContentCompressor {
    private static final int BUFFER_SIZE = 4096;
    private static final long MAX_INFLATED_BYTES = 1_000_000_000; // 1 gb maximum

    private InferenceToXContentCompressor() {}

    public static <T extends ToXContentObject> String deflate(T objectToCompress) throws IOException {
        BytesReference reference = XContentHelper.toXContent(objectToCompress, XContentType.JSON, false);
        return deflate(reference);
    }

    public static <T> T inflate(String compressedString,
                         CheckedFunction<XContentParser, T, IOException> parserFunction,
                         NamedXContentRegistry xContentRegistry) throws IOException {
        try(XContentParser parser = XContentHelper.createParser(xContentRegistry,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            inflate(compressedString, MAX_INFLATED_BYTES),
            XContentType.JSON)) {
            return parserFunction.apply(parser);
        }
    }

    static BytesReference inflate(String compressedString, long streamSize) throws IOException {
        byte[] compressedBytes = Base64.getDecoder().decode(compressedString.getBytes(StandardCharsets.UTF_8));
        InputStream gzipStream = new GZIPInputStream(new BytesArray(compressedBytes).streamInput(), BUFFER_SIZE);
        InputStream inflateStream = new SimpleBoundedInputStream(gzipStream, streamSize);
        return Streams.readFully(inflateStream);
    }

    private static String deflate(BytesReference reference) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        try (OutputStream compressedOutput = new GZIPOutputStream(out, BUFFER_SIZE)) {
            reference.writeTo(compressedOutput);
        }
        return new String(Base64.getEncoder().encode(BytesReference.toBytes(out.bytes())), StandardCharsets.UTF_8);
    }
}
