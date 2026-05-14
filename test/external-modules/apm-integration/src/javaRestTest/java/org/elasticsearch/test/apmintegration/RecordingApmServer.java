/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;

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
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * In-process recording server for the apm-integration tests. Accepts telemetry from
 * ES's OTel SDK and exposes the received messages to assertions.
 *
 * <p>This server speaks <strong>both protocols</strong>, on two independent ports:
 * <ul>
 *   <li><strong>HTTP</strong> (the {@code httpServer} field, JDK {@code HttpServer}) for OTLP/HTTP
 *       metrics ({@code /v1/metrics}), traces ({@code /v1/traces}), and APM intake
 *       ({@code /intake/v2/events}). Audit logs also have an HTTP route ({@code /v1/logs})
 *       which remains in place for completeness but isn't exercised in the current PoC.
 *   <li><strong>gRPC</strong> (the {@code grpcServer} field, {@code io.grpc.Server}) for
 *       OTLP/gRPC audit-log export. This is the production wire format for the audit-log
 *       feature (§2.5 / §4.16 of the PoC doc); the otel-delivery-gateway requires gRPC
 *       for correct load-balancing behind k8s services.
 * </ul>
 *
 * <p>Both protocols feed the same {@link #received} queue, so consumers don't need to care
 * which transport delivered a given record.
 */
@SuppressForbidden(reason = "Uses an HTTP server for testing")
public class RecordingApmServer extends ExternalResource {
    private static final Logger logger = LogManager.getLogger(RecordingApmServer.class);

    final ArrayBlockingQueue<ReceivedTelemetry> received = new ArrayBlockingQueue<>(1000);

    /**
     * The "Resource" (telemetry source identity) observed by this server. The test JVM emits
     * a single Resource, so we record the first one and ignore the rest.
     */
    private final AtomicReference<ReceivedTelemetry.ReceivedResource> resource = new AtomicReference<>();

    /** OTLP/HTTP server: metrics, traces, intake, and (legacy) the {@code /v1/logs} route. */
    private HttpServer httpServer;
    /** OTLP/gRPC server: serves {@link LogsServiceGrpc.LogsServiceImplBase} for log export. */
    private Server grpcServer;
    private final Thread messageConsumerThread = consumerThread();
    private volatile Consumer<ReceivedTelemetry> consumer;
    private volatile boolean running = true;

    @Override
    protected void before() throws Throwable {
        // OTLP/HTTP server — receives metrics, traces, intake events.
        httpServer = HttpServer.create();
        httpServer.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext("/", this::handle);
        httpServer.start();

        // OTLP/gRPC server — receives log records. Runs on a separate ephemeral port from
        // the HTTP server above; tests point ES's logs endpoint setting at {@link #getGrpcEndpoint()}.
        grpcServer = ServerBuilder.forPort(0).addService(new LogsServiceImpl()).build().start();

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
        if (httpServer != null) {
            httpServer.stop(1);
        }
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                grpcServer.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
                            case "/v1/traces" -> OtlpTracesParser.parse(requestBody).forEach(this::route);
                            case "/v1/logs" -> received.addAll(OtlpLogsParser.parse(requestBody));
                            case "/intake/v2/events" -> {
                                List<String> lines = readJsonMessages(requestBody);
                                for (String line : lines) {
                                    ApmIntakeMessageParser.parseLine(line).ifPresent(this::route);
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

    /**
     * Route a parsed event: {@link ReceivedTelemetry.ReceivedResource} goes to the
     * {@link #resource} reference (we just need one since the test JVM emits a single
     * Resource); everything else is queued for consumers.
     */
    private void route(ReceivedTelemetry msg) {
        logger.debug("telemetry received: {}", msg);
        if (msg instanceof ReceivedTelemetry.ReceivedResource r) {
            resource.compareAndSet(null, r);
        } else {
            received.add(msg);
        }
    }

    private List<String> readJsonMessages(InputStream input) {
        // parse NDJSON
        return new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8)).lines().toList();
    }

    public int getPort() {
        return httpServer.getAddress().getPort();
    }

    /**
     * Returns the HTTP address in the format "host:port", properly handling IPv6 addresses with brackets.
     */
    public String getHttpAddress() {
        String host = httpServer.getAddress().getHostString();
        if (host.contains(":")) {
            // IPv6 address needs brackets
            host = "[" + host + "]";
        }
        return host + ":" + getPort();
    }

    /**
     * @return the port the gRPC server is listening on (used by clients exporting via OTLP/gRPC).
     */
    public int getGrpcPort() {
        return grpcServer.getPort();
    }

    /**
     * Returns the gRPC endpoint as a URL the OTLP/gRPC exporter expects: {@code http://host:port}
     * (no path component, in contrast to the HTTP endpoint which includes {@code /v1/logs}).
     */
    public String getGrpcEndpoint() {
        String host = InetAddress.getLoopbackAddress().getHostAddress();
        if (host.contains(":")) {
            host = "[" + host + "]";
        }
        return "http://" + host + ":" + getGrpcPort();
    }

    /**
     * Implements the OTLP gRPC LogsService — receives {@link ExportLogsServiceRequest} messages,
     * parses each {@link LogRecord} into a {@link ReceivedTelemetry.ReceivedLog}, and feeds them
     * into the queue that the HTTP path also feeds.
     */
    private final class LogsServiceImpl extends LogsServiceGrpc.LogsServiceImplBase {
        @Override
        public void export(ExportLogsServiceRequest request, StreamObserver<ExportLogsServiceResponse> responseObserver) {
            if (running) {
                try {
                    for (ResourceLogs resourceLogs : request.getResourceLogsList()) {
                        for (ScopeLogs scopeLogs : resourceLogs.getScopeLogsList()) {
                            for (LogRecord record : scopeLogs.getLogRecordsList()) {
                                received.add(toReceivedLog(record));
                            }
                        }
                    }
                } catch (Throwable t) {
                    logger.warn("failed to handle gRPC ExportLogsServiceRequest", t);
                }
            }
            responseObserver.onNext(ExportLogsServiceResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

        private ReceivedTelemetry.ReceivedLog toReceivedLog(LogRecord record) {
            Map<String, Object> attributes = new HashMap<>();
            for (KeyValue kv : record.getAttributesList()) {
                Object value = unwrap(kv.getValue());
                if (value != null) {
                    attributes.put(kv.getKey(), value);
                }
            }
            Optional<String> traceId = record.getTraceId().isEmpty()
                ? Optional.empty()
                : Optional.of(HexFormat.of().formatHex(record.getTraceId().toByteArray()));
            return new ReceivedTelemetry.ReceivedLog(
                record.getTimeUnixNano(),
                record.getSeverityNumberValue(),
                record.getSeverityText(),
                record.getBody().getStringValue(),
                attributes,
                traceId
            );
        }

        private Object unwrap(AnyValue value) {
            return switch (value.getValueCase()) {
                case STRING_VALUE -> value.getStringValue();
                case INT_VALUE -> value.getIntValue();
                case DOUBLE_VALUE -> value.getDoubleValue();
                case BOOL_VALUE -> value.getBoolValue();
                default -> null;
            };
        }
    }

    public void addMessageConsumer(Consumer<ReceivedTelemetry> messageConsumer) {
        this.consumer = messageConsumer;
    }

    /**
     * Clears any recorded telemetry to leave the server in a clean state.
     * <p>
     * This server's lifetime coincides with that of the cluster it's attached to,
     * but that same cluster (and hence this server) may be used for multiple tests.
     * This method is intended to be used in a test class's {@code @Before}
     * and/or {@code @After} methods to prevent tests from interfering with each other.
     * <p>
     * Tests are advised to flush their telemetry, or else buffered telemetry from
     * one test may be exported during a subsequent test.
     */
    public void reset() {
        consumer = null;
        received.clear();
    }

    /**
     * @return the first {@link ReceivedTelemetry.ReceivedResource} observed in this server's
     *         lifetime, or {@code null} if no resource event has arrived yet
     */
    public ReceivedTelemetry.ReceivedResource resource() {
        return resource.get();
    }

}
