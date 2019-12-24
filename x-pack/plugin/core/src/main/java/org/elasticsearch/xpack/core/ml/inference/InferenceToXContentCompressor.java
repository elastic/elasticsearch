/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.utils.SimpleBoundedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Collection of helper methods. Similar to CompressedXContent, but this utilizes GZIP for parity with the native compression
 */
public final class InferenceToXContentCompressor {
    private static final int BUFFER_SIZE = 4096;
    private static final long MAX_INFLATED_BYTES = 1_000_000_000; // 1 gb maximum

    private InferenceToXContentCompressor() {}

    public static <T extends ToXContentObject> String deflate(T objectToCompress) throws IOException {
        BytesReference reference = XContentHelper.toXContent(objectToCompress, XContentType.JSON, false);
        return deflate(reference);
    }

    static <T> T inflate(String compressedString,
                         CheckedFunction<XContentParser, T, IOException> parserFunction,
                         NamedXContentRegistry xContentRegistry) throws IOException {
        try(XContentParser parser = XContentHelper.createParser(xContentRegistry,
            LoggingDeprecationHandler.INSTANCE,
            inflate(compressedString, MAX_INFLATED_BYTES),
            XContentType.JSON)) {
            return parserFunction.apply(parser);
        }
    }

    static Map<String, Object> inflateToMap(String compressedString) throws IOException {
        // Don't need the xcontent registry as we are not deflating named objects.
        try(XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            inflate(compressedString, MAX_INFLATED_BYTES),
            XContentType.JSON)) {
            return parser.mapOrdered();
        }
    }

    static BytesReference inflate(String compressedString, long streamSize) throws IOException {
        byte[] compressedBytes = Base64.getDecoder().decode(compressedString.getBytes(StandardCharsets.UTF_8));
        InputStream gzipStream = new GZIPInputStream(new BytesArray(compressedBytes).streamInput(), BUFFER_SIZE);
        InputStream inflateStream = new SimpleBoundedInputStream(gzipStream, streamSize);
        return Streams.readFully(inflateStream);
    }

    //Public for testing (for now)
    public static String deflate(BytesReference reference) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        try (OutputStream compressedOutput = new GZIPOutputStream(out, BUFFER_SIZE)) {
            reference.writeTo(compressedOutput);
        }
        return new String(Base64.getEncoder().encode(BytesReference.toBytes(out.bytes())), StandardCharsets.UTF_8);
    }
}
