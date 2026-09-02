/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Renders a request body as JSON while enforcing a hard byte cap and tracking heap usage via a circuit breaker.
 *
 * <p>Circuit-breaker charges accumulate during {@link #render} and are held until {@link #close()} is called,
 * so the charge covers the full lifetime of the rendered string — including any subsequent log write.
 * Callers are responsible for closing this object after the rendered string has been consumed.
 */
public final class RequestBodyRenderer implements Releasable {

    private final long maxBytes;
    @Nullable
    private final CircuitBreaker breaker;
    @Nullable
    private final String label;
    private long chargedBytes = 0;

    public RequestBodyRenderer(long maxBytes, @Nullable CircuitBreaker breaker, @Nullable String label) {
        if (breaker != null) {
            Objects.requireNonNull(label, "label required when breaker is non-null");
        }
        this.maxBytes = maxBytes;
        this.breaker = breaker;
        this.label = label;
    }

    public long maxBytes() {
        return maxBytes;
    }

    public String render(BytesReference bytes, XContentType xContentType) throws IOException {
        if (xContentType.canonical() == XContentType.JSON) {
            checkSize(0, bytes.length());
            var result = bytes.utf8ToString();
            charge(bytes.length());
            return result;
        }

        try (var os = new LimitedOutputStream()) {
            try (var parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, bytes, xContentType)) {
                parser.nextToken();
                try (var builder = XContentFactory.jsonBuilder(os)) {
                    builder.copyCurrentStructure(parser);
                }
            }
            return os.toString(StandardCharsets.UTF_8);
        }
    }

    @Override
    public void close() {
        if (breaker != null && chargedBytes > 0) {
            breaker.addWithoutBreaking(-chargedBytes);
            chargedBytes = 0;
        }
    }

    private void checkSize(long current, long additional) {
        if (maxBytes > 0 && current + additional > maxBytes) {
            throw new TooLargeBodyException(current + additional, maxBytes);
        }
    }

    private void charge(long bytes) {
        if (breaker != null) {
            // addEstimateBytesAndMaybeBreak is atomic: on trip it does not leave `bytes` charged.
            // Bytes charged by prior successful calls accumulate in chargedBytes until close() releases them.
            breaker.addEstimateBytesAndMaybeBreak(bytes, label);
            chargedBytes += bytes;
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

    private final class LimitedOutputStream extends ByteArrayOutputStream {
        @Override
        public void write(byte[] b, int off, int len) {
            checkSize(count, len);
            super.write(b, off, len);
            charge(len);
        }

        @Override
        public void write(int b) {
            checkSize(count, 1);
            super.write(b);
            charge(1);
        }
    }
}
