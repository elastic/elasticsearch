/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Renders a request body as JSON while enforcing a hard cap on the size of the rendered output.
 *
 * <p>Non-JSON bodies (e.g. SMILE, CBOR) can expand significantly when rendered as JSON, so the cap is enforced against
 * the output as it is produced rather than against the input size. Rendering stops as soon as the cap would be exceeded,
 * before the oversized output is materialized.
 */
public final class RequestBodyRenderer {

    private RequestBodyRenderer() {}

    /**
     * @param maxBytes maximum size of the rendered JSON in UTF-8 bytes; {@code 0} means unlimited
     * @throws TooLargeBodyException if the rendered output would exceed {@code maxBytes}
     */
    public static String render(BytesReference bytes, XContentType xContentType, long maxBytes) throws IOException {
        if (xContentType.canonical() == XContentType.JSON) {
            checkSize(0, bytes.length(), maxBytes);
            return bytes.utf8ToString();
        }
        try (var os = new LimitedOutputStream(maxBytes)) {
            try (var parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, bytes, xContentType)) {
                parser.nextToken();
                try (var builder = XContentFactory.jsonBuilder(os)) {
                    builder.copyCurrentStructure(parser);
                }
            }
            return os.toString(StandardCharsets.UTF_8);
        }
    }

    private static void checkSize(long current, long additional, long maxBytes) {
        if (maxBytes > 0 && current + additional > maxBytes) {
            throw new TooLargeBodyException(current + additional, maxBytes);
        }
    }

    public static final class TooLargeBodyException extends RuntimeException {
        private final long actualBytes;

        public TooLargeBodyException(long actualBytes, long maxBytes) {
            super("JSON output exceeds the configured limit of " + maxBytes + " bytes");
            this.actualBytes = actualBytes;
        }

        public long actualBytes() {
            return actualBytes;
        }
    }

    private static final class LimitedOutputStream extends ByteArrayOutputStream {
        private final long maxBytes;

        LimitedOutputStream(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        @Override
        public void write(byte[] b, int off, int len) {
            checkSize(count, len, maxBytes);
            super.write(b, off, len);
        }

        @Override
        public void write(int b) {
            checkSize(count, 1, maxBytes);
            super.write(b);
        }
    }
}
