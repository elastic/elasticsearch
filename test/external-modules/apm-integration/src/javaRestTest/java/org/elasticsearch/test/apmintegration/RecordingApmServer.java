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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.spi.XContentProvider;
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
import java.util.function.Consumer;

@SuppressForbidden(reason = "Uses an HTTP server for testing")
public class RecordingApmServer extends ExternalResource {
    private static final Logger logger = LogManager.getLogger(RecordingApmServer.class);

    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();

    final ArrayBlockingQueue<String> received = new ArrayBlockingQueue<>(1000);

    private static HttpServer server;
    private final Thread messageConsumerThread = consumerThread();
    private volatile Consumer<String> consumer;
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
            while (running) {
                if (consumer != null) {
                    try {
                        String msg = received.poll(1L, TimeUnit.SECONDS);
                        if (msg != null && msg.isEmpty() == false) {
                            consumer.accept(msg);
                        }

                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    @Override
    protected void after() {
        server.stop(1);
        consumer = null;
    }

    void stop() {
        running = false;
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (running) {
                try {
                    try (InputStream requestBody = exchange.getRequestBody()) {
                        if (requestBody != null) {
                            var read = readJsonMessages(requestBody);
                            received.addAll(read);
                        }
                    }

                } catch (RuntimeException e) {
                    logger.warn("failed to parse request", e);
                }
            }
            exchange.sendResponseHeaders(201, 0);
        }
    }

    private List<String> readJsonMessages(InputStream input) throws IOException {
        // parse NDJSON
        return new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8)).lines().toList();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public void addMessageConsumer(Consumer<String> messageConsumer) {
        this.consumer = messageConsumer;
    }
}
