/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.inference.utils.SimpleBoundedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Collection of helper methods. Similar to CompressedXContent, but this utilizes GZIP for parity with the native compression
 */
public final class InferenceToXContentCompressor {
    private static final int BUFFER_SIZE = 4096;
    // Either 25% of the configured JVM heap, or 1 GB, which ever is smaller
    private static final long MAX_INFLATED_BYTES = Math.min(
        (long) ((0.25) * JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()),
        ByteSizeValue.ofGb(1).getBytes()
    );

    private InferenceToXContentCompressor() {}

    public static <T extends ToXContentObject> BytesReference deflate(T objectToCompress) throws IOException {
        BytesReference reference = XContentHelper.toXContent(objectToCompress, XContentType.JSON, false);
        return deflate(reference);
    }

    public static <T> T inflateUnsafe(
        BytesReference compressedBytes,
        CheckedFunction<XContentParser, T, IOException> parserFunction,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        return inflate(compressedBytes, parserFunction, xContentRegistry, Long.MAX_VALUE);
    }

    public static <T> T inflate(
        BytesReference compressedBytes,
        CheckedFunction<XContentParser, T, IOException> parserFunction,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        return inflate(compressedBytes, parserFunction, xContentRegistry, MAX_INFLATED_BYTES);
    }

    static <T> T inflate(
        BytesReference compressedBytes,
        CheckedFunction<XContentParser, T, IOException> parserFunction,
        NamedXContentRegistry xContentRegistry,
        long maxBytes
    ) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                inflate(compressedBytes, maxBytes)
            )
        ) {
            return parserFunction.apply(parser);
        } catch (XContentParseException parseException) {
            SimpleBoundedInputStream.StreamSizeExceededException streamSizeCause =
                (SimpleBoundedInputStream.StreamSizeExceededException) ExceptionsHelper.unwrap(
                    parseException,
                    SimpleBoundedInputStream.StreamSizeExceededException.class
                );

            if (streamSizeCause != null) {
                // The root cause is that the model is too big.
                throw new CircuitBreakingException(
                    "Cannot parse model definition as the content is larger than the maximum stream size "
                        + "of ["
                        + streamSizeCause.getMaxBytes()
                        + "] bytes. Max stream size is 10% of the JVM heap or 1GB whichever is smallest",
                    CircuitBreaker.Durability.PERMANENT
                );
            } else {
                throw parseException;
            }
        }
    }

    static Map<String, Object> inflateToMap(BytesReference compressedBytes) throws IOException {
        // Don't need the xcontent registry as we are not deflating named objects.
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                inflate(compressedBytes, MAX_INFLATED_BYTES)
            )
        ) {
            return parser.mapOrdered();
        }
    }

    static InputStream inflate(BytesReference compressedBytes, long streamSize) throws IOException {
        // If the compressed length is already too large, it make sense that the inflated length would be as well
        // In the extremely small string case, the compressed data could actually be longer than the compressed stream
        if (compressedBytes.length() > Math.max(100L, streamSize)) {
            throw new CircuitBreakingException(
                "compressed stream is longer than maximum allowed bytes [" + streamSize + "]",
                CircuitBreaker.Durability.PERMANENT
            );
        }
        InputStream gzipStream = new GZIPInputStream(compressedBytes.streamInput(), BUFFER_SIZE);
        return new SimpleBoundedInputStream(gzipStream, streamSize);
    }

    private static BytesReference deflate(BytesReference reference) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        try (OutputStream compressedOutput = new GZIPOutputStream(out, BUFFER_SIZE)) {
            reference.writeTo(compressedOutput);
        }
        return out.bytes();
    }
}
