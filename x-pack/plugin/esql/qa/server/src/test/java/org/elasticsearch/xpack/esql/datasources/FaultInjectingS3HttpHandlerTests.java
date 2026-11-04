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
import com.sun.net.httpserver.HttpPrincipal;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FaultInjectingS3HttpHandler.FaultType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Unit tests for {@link FaultInjectingS3HttpHandler}: countdown consumption, 64 KiB response chunking,
 * closed-range mid-body faults, and the from-start ({@code bytes=0-<end>}) pin. Lives in the same qa/server
 * project as the handler so this is a plain {@link ESTestCase} (no cluster, no fixture HTTP server).
 */
@SuppressForbidden(reason = "stubs HttpExchange to exercise fault-injection countdown without HttpServer")
public class FaultInjectingS3HttpHandlerTests extends ESTestCase {

    public void testConcurrentHandleDoesNotDriveRemainingNegative() throws Exception {
        final int threads = 16;
        final int budget = 1;
        // Repeat so the old check-then-act race is not a one-off flake in CI.
        final int iterations = 50;
        for (int iter = 0; iter < iterations; iter++) {
            AtomicInteger delegateCalls = new AtomicInteger();
            AtomicInteger closes = new AtomicInteger();
            FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(exchange -> delegateCalls.incrementAndGet());
            handler.setFault(FaultType.CONNECTION_RESET, budget);

            CyclicBarrier start = new CyclicBarrier(threads);
            Thread[] workers = new Thread[threads];
            for (int i = 0; i < threads; i++) {
                workers[i] = new Thread(() -> {
                    try {
                        start.await(10, TimeUnit.SECONDS);
                        handler.handle(new StubExchange("/bucket/data.parquet", closes));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }, "fault-inject-countdown-" + iter + "-" + i);
                workers[i].start();
            }
            for (Thread worker : workers) {
                worker.join();
            }

            assertThat(
                "remaining faults must never go negative (iteration " + iter + ")",
                handler.remainingFaults(),
                greaterThanOrEqualTo(0)
            );
            assertEquals("budget must be fully consumed (iteration " + iter + ")", 0, handler.remainingFaults());
            assertEquals("exactly one injection (iteration " + iter + ")", budget, closes.get());
            assertEquals("losers fall through to the delegate (iteration " + iter + ")", threads - budget, delegateCalls.get());
        }
    }

    public void testConcurrentMidBodyHandleDoesNotDriveRemainingNegative() throws Exception {
        final int threads = 16;
        final int budget = 1;
        final int resetAfterBytes = 4;
        final byte[] payload = new byte[16];
        final int iterations = 50;
        for (int iter = 0; iter < iterations; iter++) {
            AtomicInteger injections = new AtomicInteger();
            AtomicInteger completed = new AtomicInteger();
            AtomicInteger closes = new AtomicInteger();
            FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(exchange -> {
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            });
            handler.setMidBodyResetFault(budget, resetAfterBytes, path -> true);

            CyclicBarrier start = new CyclicBarrier(threads);
            Thread[] workers = new Thread[threads];
            for (int i = 0; i < threads; i++) {
                workers[i] = new Thread(() -> {
                    try {
                        start.await(10, TimeUnit.SECONDS);
                        try {
                            handler.handle(new StubExchange("/bucket/data.parquet", closes));
                            completed.incrementAndGet();
                        } catch (IOException e) {
                            if (e.getMessage() != null && e.getMessage().contains("connection reset")) {
                                injections.incrementAndGet();
                            } else {
                                throw e;
                            }
                        }
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }, "fault-inject-mid-body-" + iter + "-" + i);
                workers[i].start();
            }
            for (Thread worker : workers) {
                worker.join();
            }

            assertThat(
                "remaining faults must never go negative (iteration " + iter + ")",
                handler.remainingFaults(),
                greaterThanOrEqualTo(0)
            );
            assertEquals("budget must be fully consumed (iteration " + iter + ")", 0, handler.remainingFaults());
            assertEquals("exactly one mid-body injection (iteration " + iter + ")", budget, injections.get());
            assertEquals("losers complete the write (iteration " + iter + ")", threads - budget, completed.get());
            assertEquals("exactly one connection close (iteration " + iter + ")", budget, closes.get());
        }
    }

    public void testPassThroughChunksLargeWrites() throws Exception {
        assertEquals(64 * 1024, FaultInjectingS3HttpHandler.RESPONSE_WRITE_CHUNK_SIZE);
        byte[] payload = patternedBytes(1024 * 1024);
        AtomicInteger closes = new AtomicInteger();
        StubExchange exchange = new StubExchange("/bucket/data.ndjson", closes);
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(ex -> {
            ex.sendResponseHeaders(200, payload.length);
            ex.getResponseBody().write(payload);
        });

        handler.handle(exchange);

        assertArrayEquals(payload, exchange.bodyBytes());
        assertThat(exchange.maxWrite(), lessThanOrEqualTo(64 * 1024));
        assertEquals(0, closes.get());
    }

    public void testMidBodyTruncationChunksWritesAndStopsAtThreshold() throws Exception {
        byte[] payload = patternedBytes(1024 * 1024);
        int resetAfterBytes = 256 * 1024;
        AtomicInteger closes = new AtomicInteger();
        StubExchange exchange = new StubExchange("/bucket/data.ndjson", closes);
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(ex -> {
            ex.sendResponseHeaders(200, payload.length);
            ex.getResponseBody().write(payload);
        });
        handler.setMidBodyResetFault(1, resetAfterBytes, path -> true);

        IOException thrown = expectThrows(IOException.class, () -> handler.handle(exchange));
        assertThat(thrown.getMessage(), containsString("connection reset"));
        assertArrayEquals(Arrays.copyOf(payload, resetAfterBytes), exchange.bodyBytes());
        assertThat(exchange.maxWrite(), lessThanOrEqualTo(64 * 1024));
        assertEquals(0, handler.remainingFaults());
        assertEquals(1, closes.get());
    }

    public void testClosedRangeOnlySkipsMissingAndOpenEndedRange() throws Exception {
        byte[] payload = patternedBytes(200);
        int resetAfterBytes = 50;
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(ex -> {
            ex.sendResponseHeaders(200, payload.length);
            ex.getResponseBody().write(payload);
        });
        handler.setMidBodyResetFault(1, resetAfterBytes, path -> true, true);

        StubExchange noRange = new StubExchange("/bucket/data.ndjson", new AtomicInteger());
        handler.handle(noRange);
        assertArrayEquals(payload, noRange.bodyBytes());
        assertEquals(1, handler.remainingFaults());

        StubExchange openEnded = new StubExchange("/bucket/data.ndjson", new AtomicInteger(), "bytes=0-");
        handler.handle(openEnded);
        assertArrayEquals(payload, openEnded.bodyBytes());
        assertEquals(1, handler.remainingFaults());

        StubExchange suffix = new StubExchange("/bucket/data.ndjson", new AtomicInteger(), "bytes=-50");
        handler.handle(suffix);
        assertArrayEquals(payload, suffix.bodyBytes());
        assertEquals(1, handler.remainingFaults());
    }

    public void testClosedRangeOnlyFaultsClosedRange() throws Exception {
        byte[] payload = patternedBytes(200);
        int resetAfterBytes = 50;
        AtomicInteger closes = new AtomicInteger();
        StubExchange closed = new StubExchange("/bucket/data.ndjson", closes, "bytes=10-199");
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(ex -> {
            ex.sendResponseHeaders(200, payload.length);
            ex.getResponseBody().write(payload);
        });
        handler.setMidBodyResetFault(1, resetAfterBytes, path -> true, true);

        IOException thrown = expectThrows(IOException.class, () -> handler.handle(closed));
        assertThat(thrown.getMessage(), containsString("connection reset"));
        assertArrayEquals(Arrays.copyOf(payload, resetAfterBytes), closed.bodyBytes());
        assertEquals(0, handler.remainingFaults());
        assertEquals(1, closes.get());
    }

    public void testFromStartClosedRangeSkipsProbeAndResumeRanges() throws Exception {
        byte[] payload = patternedBytes(200);
        int resetAfterBytes = 50;
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(ex -> {
            ex.sendResponseHeaders(200, payload.length);
            ex.getResponseBody().write(payload);
        });
        handler.setMidBodyResetFault(1, resetAfterBytes, path -> true, FaultInjectingS3HttpHandler::isFromStartClosedByteRange);

        StubExchange noRange = new StubExchange("/bucket/data.ndjson", new AtomicInteger());
        handler.handle(noRange);
        assertArrayEquals(payload, noRange.bodyBytes());
        assertEquals(1, handler.remainingFaults());

        StubExchange schemaSample = new StubExchange("/bucket/data.ndjson", new AtomicInteger(), "bytes=0-");
        handler.handle(schemaSample);
        assertArrayEquals(payload, schemaSample.bodyBytes());
        assertEquals(1, handler.remainingFaults());

        StubExchange probe = new StubExchange("/bucket/data.ndjson", new AtomicInteger(), "bytes=4096-16383");
        handler.handle(probe);
        assertArrayEquals(payload, probe.bodyBytes());
        assertEquals(1, handler.remainingFaults());

        StubExchange resume = new StubExchange("/bucket/data.ndjson", new AtomicInteger(), "bytes=50-199");
        handler.handle(resume);
        assertArrayEquals(payload, resume.bodyBytes());
        assertEquals(1, handler.remainingFaults());
    }

    public void testFromStartClosedRangeFaultsFirstSegment() throws Exception {
        byte[] payload = patternedBytes(200);
        int resetAfterBytes = 50;
        AtomicInteger closes = new AtomicInteger();
        StubExchange firstSegment = new StubExchange("/bucket/data.ndjson", closes, "bytes=0-199");
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(ex -> {
            ex.sendResponseHeaders(200, payload.length);
            ex.getResponseBody().write(payload);
        });
        handler.setMidBodyResetFault(1, resetAfterBytes, path -> true, FaultInjectingS3HttpHandler::isFromStartClosedByteRange);

        IOException thrown = expectThrows(IOException.class, () -> handler.handle(firstSegment));
        assertThat(thrown.getMessage(), containsString("connection reset"));
        assertArrayEquals(Arrays.copyOf(payload, resetAfterBytes), firstSegment.bodyBytes());
        assertEquals(0, handler.remainingFaults());
        assertEquals(1, closes.get());
    }

    private static byte[] patternedBytes(int length) {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) i;
        }
        return payload;
    }

    /**
     * Stub {@link HttpExchange}: records response-body writes (and max write size), optional Range header,
     * and close count. Path filter uses {@link #getRequestURI()}.
     */
    private static final class StubExchange extends HttpExchange {
        private final URI uri;
        private final AtomicInteger closes;
        private final Headers requestHeaders = new Headers();
        private final Headers responseHeaders = new Headers();
        private final RecordingOutputStream responseBody = new RecordingOutputStream();

        StubExchange(String path, AtomicInteger closes) {
            this(path, closes, null);
        }

        StubExchange(String path, AtomicInteger closes, String rangeHeader) {
            this.uri = URI.create("http://s3.test" + path);
            this.closes = closes;
            if (rangeHeader != null) {
                requestHeaders.add("Range", rangeHeader);
            }
        }

        byte[] bodyBytes() {
            return responseBody.toByteArray();
        }

        int maxWrite() {
            return responseBody.maxWrite();
        }

        @Override
        public URI getRequestURI() {
            return uri;
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }

        @Override
        public Headers getRequestHeaders() {
            return requestHeaders;
        }

        @Override
        public Headers getResponseHeaders() {
            return responseHeaders;
        }

        @Override
        public String getRequestMethod() {
            return "GET";
        }

        @Override
        public HttpContext getHttpContext() {
            return null;
        }

        @Override
        public InputStream getRequestBody() {
            return InputStream.nullInputStream();
        }

        @Override
        public OutputStream getResponseBody() {
            return responseBody;
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) {}

        @Override
        public InetSocketAddress getRemoteAddress() {
            return new InetSocketAddress(0);
        }

        @Override
        public int getResponseCode() {
            return -1;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return new InetSocketAddress(0);
        }

        @Override
        public String getProtocol() {
            return "HTTP/1.1";
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public void setAttribute(String name, Object value) {}

        @Override
        public void setStreams(InputStream i, OutputStream o) {}

        @Override
        public HttpPrincipal getPrincipal() {
            return null;
        }
    }

    private static final class RecordingOutputStream extends OutputStream {
        private final ByteArrayOutputStream content = new ByteArrayOutputStream();
        private int maxWrite;

        @Override
        public synchronized void write(int b) {
            content.write(b);
            maxWrite = Math.max(maxWrite, 1);
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) {
            content.write(b, off, len);
            maxWrite = Math.max(maxWrite, len);
        }

        synchronized byte[] toByteArray() {
            return content.toByteArray();
        }

        synchronized int maxWrite() {
            return maxWrite;
        }
    }
}
