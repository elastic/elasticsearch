/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
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
            while (running && Thread.currentThread().isInterrupted() == false) {
                if (consumer != null) {
                    try {
                        String msg = received.poll(1L, TimeUnit.SECONDS);
                        if (msg != null && msg.isEmpty() == false) {
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
                        if ("/v1/metrics".equals(path)) {
                            parseOtlpMetrics(requestBody);
                        } else {
                            received.addAll(readJsonMessages(requestBody));
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

    /**
     * Parses OTLP protobuf metrics and normalizes them into the same JSON shape that the APM agent produces.
     */
    private void parseOtlpMetrics(InputStream input) throws IOException {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(input);
        for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                String scopeName = scopeMetrics.getScope().getName();
                for (Metric metric : scopeMetrics.getMetricsList()) {
                    switch (metric.getDataCase()) {
                        case SUM, GAUGE -> {
                            var dataPoints = metric.getDataCase() == Metric.DataCase.SUM
                                ? metric.getSum().getDataPointsList()
                                : metric.getGauge().getDataPointsList();
                            for (NumberDataPoint dp : dataPoints) {
                                var builder = XContentFactory.jsonBuilder().startObject().startObject("metricset");
                                writeTags(builder, scopeName, dp.getAttributesList());
                                builder.startObject("samples").startObject(metric.getName());
                                switch (dp.getValueCase()) {
                                    case AS_DOUBLE -> builder.field("value", dp.getAsDouble());
                                    case AS_INT -> builder.field("value", dp.getAsInt());
                                }
                                builder.endObject().endObject();
                                received.offer(Strings.toString(builder.endObject().endObject()));
                            }
                        }
                        case HISTOGRAM -> {
                            for (HistogramDataPoint dp : metric.getHistogram().getDataPointsList()) {
                                var builder = XContentFactory.jsonBuilder().startObject().startObject("metricset");
                                writeTags(builder, scopeName, dp.getAttributesList());
                                builder.startObject("samples").startObject(metric.getName());
                                builder.field("counts", dp.getBucketCountsList());
                                builder.endObject().endObject();
                                received.offer(Strings.toString(builder.endObject().endObject()));
                            }
                        }
                        default -> {
                            var builder = XContentFactory.jsonBuilder().startObject().startObject("metricset");
                            writeTags(builder, scopeName, List.of());
                            builder.startObject("samples").startObject(metric.getName()).endObject().endObject();
                            received.offer(Strings.toString(builder.endObject().endObject()));
                        }
                    }
                }
            }
        }
    }

    private static void writeTags(XContentBuilder builder, String scopeName, List<KeyValue> attributes) throws IOException {
        builder.startObject("tags");
        builder.field("otel_instrumentation_scope_name", scopeName);
        for (KeyValue kv : attributes) {
            switch (kv.getValue().getValueCase()) {
                case STRING_VALUE -> builder.field(kv.getKey(), kv.getValue().getStringValue());
                case INT_VALUE -> builder.field(kv.getKey(), kv.getValue().getIntValue());
                case DOUBLE_VALUE -> builder.field(kv.getKey(), kv.getValue().getDoubleValue());
                case BOOL_VALUE -> builder.field(kv.getKey(), kv.getValue().getBoolValue());
                default -> {
                }
            }
        }
        builder.endObject();
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

    public void addMessageConsumer(Consumer<String> messageConsumer) {
        this.consumer = messageConsumer;
    }
}
