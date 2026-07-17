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
import io.grpc.Status;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.fixtures.tls.TestTlsCertificate;
import org.junit.rules.ExternalResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@SuppressForbidden(reason = "Uses an HTTP server for testing; creates temp files for TLS certificates")
public class RecordingApmServer extends ExternalResource {
    private static final Logger logger = LogManager.getLogger(RecordingApmServer.class);

    /** Creates a server that requires mTLS on its gRPC endpoint. */
    public static RecordingApmServer withMtls() {
        return new RecordingApmServer(true);
    }

    private final boolean mtlsEnabled;
    private Path mtlsTempDir;
    private Path mtlsServerCaCertPath;
    private Path mtlsClientCertPath;
    private Path mtlsClientKeyPath;

    public RecordingApmServer() {
        this(false);
    }

    private RecordingApmServer(boolean mtlsEnabled) {
        this.mtlsEnabled = mtlsEnabled;
    }

    private final BlockingQueue<ReceivedTelemetry> received = new LinkedBlockingQueue<>();

    /**
     * The "Resource" (telemetry source identity) observed by this server. The test JVM emits
     * a single Resource, so we record the first one and ignore the rest.
     */
    private final AtomicReference<ReceivedTelemetry.ReceivedResource> resource = new AtomicReference<>();

    private HttpServer server;
    private Server grpcServer;
    private final Thread messageConsumerThread = consumerThread();
    private volatile Consumer<ReceivedTelemetry> consumer;
    private volatile boolean running = true;
    private volatile int responseCode = 201;

    @Override
    protected void before() throws Throwable {
        server = HttpServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();

        if (mtlsEnabled) {
            mtlsTempDir = Files.createTempDirectory("grpc-mtls-");
            TestTlsCertificate serverCert = TestTlsCertificate.generate("localhost");
            TestTlsCertificate clientCert = TestTlsCertificate.generate("localhost");

            mtlsServerCaCertPath = mtlsTempDir.resolve("server.crt");
            Path serverKeyPath = mtlsTempDir.resolve("server.key");
            mtlsClientCertPath = mtlsTempDir.resolve("client.crt");
            mtlsClientKeyPath = mtlsTempDir.resolve("client.key");

            Files.copy(serverCert.getPemCertificateStream(), mtlsServerCaCertPath);
            Files.copy(serverCert.getPemPrivateKeyStream(), serverKeyPath);
            Files.copy(clientCert.getPemCertificateStream(), mtlsClientCertPath);
            Files.copy(clientCert.getPemPrivateKeyStream(), mtlsClientKeyPath);

            try (
                InputStream cert = serverCert.getPemCertificateStream();
                InputStream key = serverCert.getPemPrivateKeyStream();
                InputStream clientTrust = clientCert.getPemCertificateStream()
            ) {
                SslContext sslContext = GrpcSslContexts.configure(
                    SslContextBuilder.forServer(cert, key).trustManager(clientTrust).clientAuth(ClientAuth.REQUIRE)
                ).build();
                grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                    .sslContext(sslContext)
                    .addService(new LogsServiceImpl())
                    .addService(new MetricsServiceImpl())
                    .addService(new TraceServiceImpl())
                    .build()
                    .start();
            }
        } else {
            grpcServer = ServerBuilder.forPort(0)
                .addService(new LogsServiceImpl())
                .addService(new MetricsServiceImpl())
                .addService(new TraceServiceImpl())
                .build()
                .start();
        }

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
            server.stop(30);
        }
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                grpcServer.awaitTermination(30, TimeUnit.SECONDS);
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
        if (mtlsTempDir != null) {
            try (var paths = Files.walk(mtlsTempDir)) {
                paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        logger.warn("failed to delete TLS temp file [{}]", p, e);
                    }
                });
            } catch (IOException e) {
                logger.warn("failed to clean up mTLS temp dir [{}]", mtlsTempDir, e);
            }
        }
    }

    /**
     * Override the HTTP response code for all subsequent responses. Codes {@code >= 400}
     * short-circuit telemetry parsing to simulate APM server failures.
     * Call {@link #clearResponseCode()} to restore default.
     */
    public void setResponseCode(int code) {
        this.responseCode = code;
    }

    /** Restore the default response (201) for subsequent requests. */
    public void clearResponseCode() {
        this.responseCode = 201;
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            int responseCode = this.responseCode;
            if (responseCode >= 400) {
                exchange.getRequestBody().readAllBytes();
                exchange.sendResponseHeaders(responseCode, 0);
                return;
            }

            String path = exchange.getRequestURI().getPath();
            if (running) {
                try (InputStream requestBody = exchange.getRequestBody()) {
                    if (requestBody != null) {
                        // The HTTP server only serves the legacy APM-agent intake; all OTel SDK signals
                        // (metrics, traces, logs) export over OTLP/gRPC and are handled by the gRPC services below.
                        switch (path) {
                            case "/intake/v2/events" -> {
                                List<String> lines = readJsonMessages(requestBody);
                                for (String line : lines) {
                                    ApmIntakeMessageParser.parseLine(line).ifPresent(this::route);
                                }
                            }
                            default -> logger.debug("ignoring request to unhandled path [{}]", path);
                        }
                    }
                }
            }
            exchange.sendResponseHeaders(responseCode, 0);
        } catch (Throwable t) {
            logger.error("Unexpected error caught when serving HTTP request", t);
            throw t;
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

    /**
     * Returns the gRPC endpoint URL the OTLP/gRPC exporter expects: {@code scheme://host:port},
     * with no path component. For mTLS servers the scheme is {@code https} and the host is
     * {@code localhost} so OkHttp's hostname verifier matches the {@code DNS:localhost} SAN
     * in the server certificate.
     */
    public String getGrpcEndpoint() {
        if (mtlsEnabled) {
            return "https://localhost:" + grpcServer.getPort();
        }
        String host = InetAddress.getLoopbackAddress().getHostAddress();
        if (host.contains(":")) {
            host = "[" + host + "]";
        }
        return "http://" + host + ":" + grpcServer.getPort();
    }

    /** Path of the server's self-signed CA cert PEM file; the ES node trusts this to verify the server. */
    public String getMtlsServerCaCertPath() {
        return mtlsServerCaCertPath.toString();
    }

    /** Path of the client cert PEM file; the ES node presents this during the mTLS handshake. */
    public String getMtlsClientCertPath() {
        return mtlsClientCertPath.toString();
    }

    /** Path of the client private key PEM file. */
    public String getMtlsClientKeyPath() {
        return mtlsClientKeyPath.toString();
    }

    private final class LogsServiceImpl extends LogsServiceGrpc.LogsServiceImplBase {
        @Override
        public void export(ExportLogsServiceRequest request, StreamObserver<ExportLogsServiceResponse> responseObserver) {
            if (running) {
                try {
                    OtlpLogsParser.parse(request).forEach(RecordingApmServer.this::route);
                } catch (Throwable t) {
                    logger.warn("failed to handle gRPC ExportLogsServiceRequest", t);
                }
            }
            responseObserver.onNext(ExportLogsServiceResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

    }

    private final class MetricsServiceImpl extends MetricsServiceGrpc.MetricsServiceImplBase {
        @Override
        public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver) {
            if (responseCode >= 400) {
                responseObserver.onError(Status.UNAVAILABLE.withDescription("injected failure").asRuntimeException());
                return;
            }
            if (running) {
                try {
                    OtlpMetricsParser.parse(request).forEach(RecordingApmServer.this::route);
                } catch (Throwable t) {
                    logger.warn("failed to handle gRPC ExportMetricsServiceRequest", t);
                }
            }
            responseObserver.onNext(ExportMetricsServiceResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    private final class TraceServiceImpl extends TraceServiceGrpc.TraceServiceImplBase {
        @Override
        public void export(ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> responseObserver) {
            if (responseCode >= 400) {
                responseObserver.onError(Status.UNAVAILABLE.withDescription("injected failure").asRuntimeException());
                return;
            }
            if (running) {
                try {
                    OtlpTracesParser.parse(request).forEach(RecordingApmServer.this::route);
                } catch (Throwable t) {
                    logger.warn("failed to handle gRPC ExportTraceServiceRequest", t);
                }
            }
            responseObserver.onNext(ExportTraceServiceResponse.getDefaultInstance());
            responseObserver.onCompleted();
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
        clearResponseCode();
    }

    /**
     * @return the first {@link ReceivedTelemetry.ReceivedResource} observed in this server's
     *         lifetime, or {@code null} if no resource event has arrived yet
     */
    public ReceivedTelemetry.ReceivedResource resource() {
        return resource.get();
    }

}
