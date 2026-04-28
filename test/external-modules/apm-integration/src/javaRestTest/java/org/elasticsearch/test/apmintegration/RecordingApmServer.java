/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.junit.rules.ExternalResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@SuppressForbidden(reason = "Uses an HTTP server for testing")
public class RecordingApmServer extends ExternalResource {
    private static final Logger logger = LogManager.getLogger(RecordingApmServer.class);

    final ArrayBlockingQueue<ReceivedTelemetry> received = new ArrayBlockingQueue<>(1000);

    /**
     * First {@link ReceivedTelemetry.ReceivedResource} observed in this server's lifetime.
     * Resource is per-JVM, not per-span; the APM agent sends it as line 1 of every intake request,
     * so first-write-wins is safe. Routed via this setter rather than the {@link #received} queue to
     * keep tests from racing on arrival order with transactions.
     * <p>
     * <b>Known limitation:</b> first-write-wins pins the resource for the lifetime of this server
     * instance. Tests asserting on resource <em>values</em> (rather than just key presence) across
     * differing JVM configurations must use a fresh {@link RecordingApmServer} per scenario.
     */
    private final AtomicReference<ReceivedTelemetry.ReceivedResource> resource = new AtomicReference<>();

    private static HttpServer server;
    private final Thread messageConsumerThread = consumerThread();
    private volatile Consumer<ReceivedTelemetry> consumer;
    private volatile boolean running = true;

    @Override
    protected void before() throws Throwable {
        server = HttpServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();

        messageConsumerThread.start();
    }

    private Thread consumerThread() {
        return new Thread(() -> {
            while (running && Thread.currentThread().isInterrupted() == false) {
                if (consumer != null) {
                    try {
                        ReceivedTelemetry msg = received.poll(1L, TimeUnit.SECONDS);
                        if (msg != null) {
                            consumer.accept(msg);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        logger.warn("failed to process message", e);
                    }
                }
            }
        });
    }

    @Override
    protected void after() {
        running = false;
        messageConsumerThread.interrupt();
        if (server != null) {
            server.stop(1);
        }
        consumer = null;
        try {
            messageConsumerThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            if (running) {
                try (InputStream requestBody = exchange.getRequestBody()) {
                    if (requestBody != null) {
                        switch (path) {
                            case "/v1/metrics" -> received.addAll(OtlpMetricsParser.parse(requestBody));
                            case "/v1/traces" -> received.addAll(OtlpTracesParser.parse(requestBody));
                            case "/intake/v2/events" -> {
                                List<String> lines = readJsonMessages(requestBody);
                                for (String line : lines) {
                                    ApmIntakeMessageParser.parseLine(line).ifPresent(msg -> {
                                        logger.debug("APM telemetry received: {}", msg);
                                        if (msg instanceof ReceivedTelemetry.ReceivedResource r) {
                                            // Resource is per-JVM; first-write-wins. Not queued so tests don't
                                            // race between metadata and transactions from the same intake request.
                                            resource.compareAndSet(null, r);
                                        } else {
                                            received.add(msg);
                                        }
                                    });
                                }
                            }
                            default -> logger.debug("ignoring request to unhandled path [{}]", path);
                        }
                    }
                } catch (Throwable t) {
                    // The lifetime of HttpServer makes message handling "brittle": we need to start handling and recording received
                    // messages before the test starts running. We should also stop handling them before the test ends (and the test
                    // cluster is torn down), or we may run into IOException as the communication channel is interrupted.
                    // Coordinating the lifecycle of the mock HttpServer and of the test ES cluster is difficult and error-prone, so
                    // we just handle Throwable and don't care (log, but don't care): if we have an error in communicating to/from
                    // the mock server while the test is running, the test would fail anyway as the expected messages will not arrive, and
                    // if we have an error outside the test scope (before or after) that is OK.
                    logger.warn("failed to parse request", t);
                }
            }
            exchange.sendResponseHeaders(201, 0);
        }
    }

    private List<String> readJsonMessages(InputStream input) {
        // parse NDJSON
        return new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8)).lines().toList();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    /**
     * Returns the HTTP address in the format "host:port", properly handling IPv6 addresses with brackets.
     */
    public String getHttpAddress() {
        String host = server.getAddress().getHostString();
        if (host.contains(":")) {
            // IPv6 address needs brackets
            host = "[" + host + "]";
        }
        return host + ":" + getPort();
    }

    public void addMessageConsumer(Consumer<ReceivedTelemetry> messageConsumer) {
        this.consumer = messageConsumer;
    }

    /**
     * @return the first {@link ReceivedTelemetry.ReceivedResource} observed in this server's
     *         lifetime, or {@code null} if no metadata event has arrived yet
     */
    public ReceivedTelemetry.ReceivedResource resource() {
        return resource.get();
    }

}
