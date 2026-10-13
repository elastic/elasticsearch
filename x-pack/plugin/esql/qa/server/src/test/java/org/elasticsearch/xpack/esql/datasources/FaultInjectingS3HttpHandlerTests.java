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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Unit tests for {@link FaultInjectingS3HttpHandler} countdown consumption.
 * Lives in the same qa/server project as the handler so this is a plain {@link ESTestCase}
 * (no cluster, no fixture HTTP server).
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

    /**
     * Minimal {@link HttpExchange}: {@link FaultType#CONNECTION_RESET} only needs {@code close()},
     * and the path filter only needs {@link #getRequestURI()}.
     */
    private static final class StubExchange extends HttpExchange {
        private final URI uri;
        private final AtomicInteger closes;

        StubExchange(String path, AtomicInteger closes) {
            this.uri = URI.create("http://s3.test" + path);
            this.closes = closes;
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
            return new Headers();
        }

        @Override
        public Headers getResponseHeaders() {
            return new Headers();
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
            return OutputStream.nullOutputStream();
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
}
