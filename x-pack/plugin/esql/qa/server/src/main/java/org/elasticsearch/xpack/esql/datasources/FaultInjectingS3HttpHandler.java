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
import java.util.regex.Pattern;

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

    /**
     * Cap on a single {@code write} to the JDK {@code HttpServer} body. {@code WriteStream.write} allocates
     * {@code 2*len}, so an unsplit tens-of-MB blob write OOMs a 512m fixture JVM. 64 KiB keeps the extra
     * allocation bounded on both pass-through and truncated mid-body responses.
     */
    static final int RESPONSE_WRITE_CHUNK_SIZE = 64 * 1024;

    /** Closed {@code Range: bytes=<start>-<end>}. Not missing, not {@code bytes=N-}, not suffix {@code bytes=-N}. */
    private static final Pattern CLOSED_BYTE_RANGE = Pattern.compile("bytes=\\d+-\\d+");

    /**
     * First-segment closed range {@code bytes=0-<end>}. {@code computeSegments} probes start at a stride
     * {@code > 0}; finite resumes are {@code newStream(resumeFrom, remaining)} with {@code resumeFrom > 0}.
     */
    private static final Pattern FROM_START_CLOSED_BYTE_RANGE = Pattern.compile("bytes=0-\\d+");

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
        this.activeFault = new FaultConfig(type, new AtomicInteger(count), pathFilter, DEFAULT_MID_BODY_RESET_AFTER_BYTES, null);
    }

    /**
     * Arm a {@link FaultType#CONNECTION_RESET_MID_BODY} fault that serves {@code resetAfterBytes} bytes of the
     * real response body before dropping the connection. Reads that complete under {@code resetAfterBytes}
     * (e.g. small schema-inference or record-boundary probes) pass through untouched and do not consume the
     * fault budget — only a read that crosses the threshold (a segment body read) is faulted. This reaches a
     * stream-consumer that has already opened the object, exercising mid-read recovery rather than open-retry.
     */
    public void setMidBodyResetFault(int count, int resetAfterBytes, Predicate<String> pathFilter) {
        setMidBodyResetFault(count, resetAfterBytes, pathFilter, false);
    }

    /**
     * Like {@link #setMidBodyResetFault(int, int, Predicate)} but, when {@code closedRangesOnly} is true, only
     * closed {@code Range: bytes=<start>-<end>} requests are truncated. Missing Range (metadata / schema-sample
     * full GETs), open-ended {@code bytes=N-}, and suffix {@code bytes=-N} pass through and do not consume the
     * budget. Finite-length {@code newStream(pos, len)} — segment body, advancing-boundary probes, and finite
     * resumes — is the closed-range form; a stride probe is also closed, so this does not isolate segment GETs.
     */
    public void setMidBodyResetFault(int count, int resetAfterBytes, Predicate<String> pathFilter, boolean closedRangesOnly) {
        setMidBodyResetFault(count, resetAfterBytes, pathFilter, closedRangesOnly ? FaultInjectingS3HttpHandler::isClosedByteRange : null);
    }

    /**
     * Mid-body reset restricted by {@code rangeFilter} on the raw {@code Range} header (null when missing).
     * A {@code false} result passes through and does not consume the budget. Use
     * {@link #isFromStartClosedByteRange(String)} to pin the fault on the first segment GET
     * ({@code bytes=0-<end>}), skipping stride probes and finite resumes that start past 0.
     */
    public void setMidBodyResetFault(int count, int resetAfterBytes, Predicate<String> pathFilter, Predicate<String> rangeFilter) {
        this.activeFault = new FaultConfig(
            FaultType.CONNECTION_RESET_MID_BODY,
            new AtomicInteger(count),
            pathFilter,
            resetAfterBytes,
            rangeFilter
        );
    }

    /** Closed {@code Range: bytes=<start>-<end>}. */
    public static boolean isClosedByteRange(String range) {
        return range != null && CLOSED_BYTE_RANGE.matcher(range).matches();
    }

    /**
     * Closed range from byte 0 ({@code bytes=0-<end>}). First-segment GETs use this form; probes start at a
     * stride {@code > 0}; a finite resume is {@code newStream(resumeFrom, remaining)} with {@code resumeFrom > 0}.
     * A {@code bytes=0-0} size probe also matches but cannot cross a positive truncate threshold.
     */
    public static boolean isFromStartClosedByteRange(String range) {
        return range != null && FROM_START_CLOSED_BYTE_RANGE.matcher(range).matches();
    }

    public void clearFault() {
        this.activeFault = null;
    }

    public int remainingFaults() {
        FaultConfig fault = this.activeFault;
        return fault == null ? 0 : fault.remaining.get();
    }

    /**
     * Atomically consume one fault slot. Returns the previous remaining count;
     * the caller should inject iff the result is {@code > 0}. Never drives remaining below 0.
     */
    private static int tryConsumeFault(AtomicInteger remaining) {
        return remaining.getAndUpdate(n -> n > 0 ? n - 1 : n);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // Always chunk the JDK HttpServer body so a tens-of-MB blob.writeTo cannot allocate 2*len.
        exchange = new ChunkingHttpExchange(exchange);
        FaultConfig fault = this.activeFault;
        if (fault != null && fault.remaining.get() > 0 && fault.pathFilter.test(exchange.getRequestURI().getPath())) {
            if (fault.type == FaultType.CONNECTION_RESET_MID_BODY) {
                if (fault.rangeFilter != null && fault.rangeFilter.test(exchange.getRequestHeaders().getFirst("Range")) == false) {
                    delegate.handle(exchange);
                    return;
                }
                // Serve the real response but truncate once the body crosses the threshold; the wrapper
                // decrements the budget only if/when it actually truncates, so sub-threshold probe reads
                // leave the fault armed for the next (larger) segment read. Truncating wraps the already-
                // chunked exchange so a 1 MiB cutoff still hits 64 KiB JDK buffers.
                delegate.handle(new TruncatingHttpExchange(exchange, fault.resetAfterBytes, fault.remaining));
                return;
            }
            int previous = tryConsumeFault(fault.remaining);
            if (previous > 0) {
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

    private record FaultConfig(
        FaultType type,
        AtomicInteger remaining,
        Predicate<String> pathFilter,
        int resetAfterBytes,
        Predicate<String> rangeFilter
    ) {}

    /**
     * Forwards every {@link HttpExchange} operation to {@code delegate}. Subclasses override the one
     * method they need (response body wrapping).
     */
    @SuppressForbidden(reason = "wraps HttpExchange for S3 fixture fault injection")
    private abstract static class DelegatingHttpExchange extends HttpExchange {
        protected final HttpExchange delegate;

        DelegatingHttpExchange(HttpExchange delegate) {
            this.delegate = delegate;
        }

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
        public OutputStream getResponseBody() {
            return delegate.getResponseBody();
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
     * Splits response-body writes to {@link #RESPONSE_WRITE_CHUNK_SIZE} so the JDK {@code HttpServer}
     * never allocates {@code 2*len} for a whole-blob write.
     */
    @SuppressForbidden(reason = "wraps HttpExchange to chunk the response body for the JDK HttpServer")
    private static final class ChunkingHttpExchange extends DelegatingHttpExchange {
        ChunkingHttpExchange(HttpExchange delegate) {
            super(delegate);
        }

        @Override
        public OutputStream getResponseBody() {
            return new ChunkingOutputStream(delegate.getResponseBody());
        }
    }

    private static final class ChunkingOutputStream extends OutputStream {
        private final OutputStream delegate;

        ChunkingOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            while (len > 0) {
                int n = Math.min(len, RESPONSE_WRITE_CHUNK_SIZE);
                delegate.write(b, off, n);
                off += n;
                len -= n;
            }
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * Wraps an {@link HttpExchange} so the response body is cut off after {@code resetAfterBytes} bytes and the
     * connection is dropped, simulating a server-side idle/connection reset on an already-open object stream.
     * All other exchange operations delegate to the wrapped exchange.
     */
    @SuppressForbidden(reason = "wraps HttpExchange to truncate the response body for fault injection")
    private static final class TruncatingHttpExchange extends DelegatingHttpExchange {
        private final int resetAfterBytes;
        private final AtomicInteger remaining;

        TruncatingHttpExchange(HttpExchange delegate, int resetAfterBytes, AtomicInteger remaining) {
            super(delegate);
            this.resetAfterBytes = resetAfterBytes;
            this.remaining = remaining;
        }

        @Override
        public OutputStream getResponseBody() {
            return new TruncatingOutputStream(delegate.getResponseBody(), delegate, resetAfterBytes, remaining);
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
            // Cross the threshold. Consume the budget atomically so concurrent mid-body
            // GETs cannot drive remaining below 0. Only the winner truncates; losers
            // serve this write in full.
            int previous = tryConsumeFault(remaining);
            if (previous > 0) {
                if (room > 0) {
                    delegate.write(b, off, (int) room);
                    written += room;
                }
                truncated = true;
                logger.debug("injecting mid-body connection reset after [{}] bytes, remaining [{}]", written, remaining.get());
                exchange.close();
                throw new IOException("connection reset (injected mid-body fault)");
            }
            delegate.write(b, off, len);
            written += len;
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
