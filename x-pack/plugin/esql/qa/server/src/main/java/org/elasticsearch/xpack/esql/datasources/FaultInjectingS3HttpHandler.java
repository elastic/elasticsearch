/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpPrincipal;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * HTTP handler wrapper that injects configurable faults into S3 requests.
 * Supports HTTP 503, HTTP 500, connection reset at open, and connection reset
 * mid-body fault types with countdown-based auto-clearing and path-based filtering.
 */
@SuppressForbidden(reason = "uses HttpServer to emulate a faulty S3 endpoint")
public class FaultInjectingS3HttpHandler implements HttpHandler {

    private static final Logger logger = LogManager.getLogger(FaultInjectingS3HttpHandler.class);

    /** Default number of body bytes served before a {@link FaultType#CONNECTION_RESET_MID_BODY} drops the connection. */
    public static final int DEFAULT_MID_BODY_RESET_AFTER_BYTES = 1024 * 1024;

    private final HttpHandler delegate;
    private volatile FaultConfig activeFault;

    public FaultInjectingS3HttpHandler(HttpHandler delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        this.delegate = delegate;
    }

    public void setFault(FaultType type, int count) {
        setFault(type, count, path -> true);
    }

    public void setFault(FaultType type, int count, Predicate<String> pathFilter) {
        this.activeFault = new FaultConfig(type, new AtomicInteger(count), pathFilter, DEFAULT_MID_BODY_RESET_AFTER_BYTES);
    }

    /**
     * Arm a {@link FaultType#CONNECTION_RESET_MID_BODY} fault that serves {@code resetAfterBytes} bytes of the
     * real response body before dropping the connection. Reads that complete under {@code resetAfterBytes}
     * (e.g. small schema-inference or record-boundary probes) pass through untouched and do not consume the
     * fault budget — only a read that crosses the threshold (a segment body read) is faulted. This reaches a
     * stream-consumer that has already opened the object, exercising mid-read recovery rather than open-retry.
     */
    public void setMidBodyResetFault(int count, int resetAfterBytes, Predicate<String> pathFilter) {
        this.activeFault = new FaultConfig(FaultType.CONNECTION_RESET_MID_BODY, new AtomicInteger(count), pathFilter, resetAfterBytes);
    }

    public void clearFault() {
        this.activeFault = null;
    }

    public int remainingFaults() {
        FaultConfig fault = this.activeFault;
        return fault == null ? 0 : fault.remaining.get();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        FaultConfig fault = this.activeFault;
        if (fault != null && fault.remaining.get() > 0 && fault.pathFilter.test(exchange.getRequestURI().getPath())) {
            if (fault.type == FaultType.CONNECTION_RESET_MID_BODY) {
                // Serve the real response but truncate once the body crosses the threshold; the wrapper
                // decrements the budget only if/when it actually truncates, so sub-threshold probe reads
                // leave the fault armed for the next (larger) segment read.
                delegate.handle(new TruncatingHttpExchange(exchange, fault.resetAfterBytes, fault.remaining));
                return;
            }
            if (fault.remaining.decrementAndGet() >= 0) {
                logger.debug(
                    "injecting fault [{}] for path [{}], remaining [{}]",
                    fault.type,
                    exchange.getRequestURI().getPath(),
                    fault.remaining.get()
                );
                injectFault(exchange, fault.type);
                return;
            }
        }
        delegate.handle(exchange);
    }

    private static void injectFault(HttpExchange exchange, FaultType type) throws IOException {
        switch (type) {
            case HTTP_503 -> sendErrorResponse(exchange, 503, "Service Unavailable", "SlowDown", "Reduce your request rate");
            case HTTP_500 -> sendErrorResponse(exchange, 500, "Internal Server Error", "InternalError", "Internal server error");
            case CONNECTION_RESET -> exchange.close();
            // Handled in handle() via the truncating wrapper before injectFault is reached.
            case CONNECTION_RESET_MID_BODY -> throw new AssertionError("CONNECTION_RESET_MID_BODY must be handled in handle()");
        }
    }

    private static void sendErrorResponse(HttpExchange exchange, int statusCode, String statusText, String code, String message)
        throws IOException {
        String body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<Error><Code>"
            + code
            + "</Code><Message>"
            + message
            + "</Message></Error>";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/xml");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    public enum FaultType {
        HTTP_503,
        HTTP_500,
        CONNECTION_RESET,
        /** Drop the connection partway through serving the response body (a reset on an already-open stream). */
        CONNECTION_RESET_MID_BODY
    }

    private record FaultConfig(FaultType type, AtomicInteger remaining, Predicate<String> pathFilter, int resetAfterBytes) {}

    /**
     * Wraps an {@link HttpExchange} so the response body is cut off after {@code resetAfterBytes} bytes and the
     * connection is dropped, simulating a server-side idle/connection reset on an already-open object stream.
     * All other exchange operations delegate to the wrapped exchange.
     */
    @SuppressForbidden(reason = "wraps HttpExchange to truncate the response body for fault injection")
    private static final class TruncatingHttpExchange extends HttpExchange {
        private final HttpExchange delegate;
        private final int resetAfterBytes;
        private final AtomicInteger remaining;

        TruncatingHttpExchange(HttpExchange delegate, int resetAfterBytes, AtomicInteger remaining) {
            this.delegate = delegate;
            this.resetAfterBytes = resetAfterBytes;
            this.remaining = remaining;
        }

        @Override
        public OutputStream getResponseBody() {
            return new TruncatingOutputStream(delegate.getResponseBody(), delegate, resetAfterBytes, remaining);
        }

        // --- everything else delegates ---
        @Override
        public Headers getRequestHeaders() {
            return delegate.getRequestHeaders();
        }

        @Override
        public Headers getResponseHeaders() {
            return delegate.getResponseHeaders();
        }

        @Override
        public URI getRequestURI() {
            return delegate.getRequestURI();
        }

        @Override
        public String getRequestMethod() {
            return delegate.getRequestMethod();
        }

        @Override
        public HttpContext getHttpContext() {
            return delegate.getHttpContext();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public InputStream getRequestBody() {
            return delegate.getRequestBody();
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) throws IOException {
            delegate.sendResponseHeaders(rCode, responseLength);
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return delegate.getRemoteAddress();
        }

        @Override
        public int getResponseCode() {
            return delegate.getResponseCode();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return delegate.getLocalAddress();
        }

        @Override
        public String getProtocol() {
            return delegate.getProtocol();
        }

        @Override
        public Object getAttribute(String name) {
            return delegate.getAttribute(name);
        }

        @Override
        public void setAttribute(String name, Object value) {
            delegate.setAttribute(name, value);
        }

        @Override
        public void setStreams(InputStream i, OutputStream o) {
            delegate.setStreams(i, o);
        }

        @Override
        public HttpPrincipal getPrincipal() {
            return delegate.getPrincipal();
        }
    }

    /**
     * Passes writes through until {@code resetAfterBytes} bytes have been written, then drops the connection
     * and fails the write so the server stops mid-body. The fault budget is decremented exactly once, when the
     * threshold is first crossed.
     */
    private static final class TruncatingOutputStream extends OutputStream {
        private final OutputStream delegate;
        private final HttpExchange exchange;
        private final int resetAfterBytes;
        private final AtomicInteger remaining;
        private long written = 0;
        private boolean truncated = false;

        TruncatingOutputStream(OutputStream delegate, HttpExchange exchange, int resetAfterBytes, AtomicInteger remaining) {
            this.delegate = delegate;
            this.exchange = exchange;
            this.resetAfterBytes = resetAfterBytes;
            this.remaining = remaining;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] { (byte) b }, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (truncated) {
                throw new IOException("connection reset (injected mid-body fault)");
            }
            long room = resetAfterBytes - written;
            if (len <= room) {
                delegate.write(b, off, len);
                written += len;
                return;
            }
            // Cross the threshold: write what fits, then drop the connection mid-body.
            if (room > 0) {
                delegate.write(b, off, (int) room);
                written += room;
            }
            truncated = true;
            // Only consume the budget when we actually fault (so sub-threshold probe reads don't).
            int left = remaining.decrementAndGet();
            logger.debug("injecting mid-body connection reset after [{}] bytes, remaining [{}]", written, left);
            exchange.close();
            throw new IOException("connection reset (injected mid-body fault)");
        }

        @Override
        public void flush() throws IOException {
            if (truncated == false) {
                delegate.flush();
            }
        }

        @Override
        public void close() throws IOException {
            if (truncated == false) {
                delegate.close();
            }
        }
    }
}
