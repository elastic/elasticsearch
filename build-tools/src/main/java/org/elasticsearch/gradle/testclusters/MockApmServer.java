/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.testclusters;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A mock OTLP/gRPC collector that logs the metrics and spans it receives, so that
 * {@code gradlew run --with-apm-server} can show telemetry without a real APM server.
 * <p>
 * Note: automated integration tests use {@code RecordingApmServer} (in {@code test/external-modules/apm-integration}),
 * not this class.
 */
@NotThreadSafe
public class MockApmServer {
    private static final Logger logger = Logging.getLogger(MockApmServer.class);

    private final Pattern metricFilter;

    private Server grpcInstance;

    public MockApmServer(String metricFilter) {
        this.metricFilter = createWildcardPattern(metricFilter);
    }

    private Pattern createWildcardPattern(String filter) {
        if (filter == null || filter.isEmpty()) {
            return null;
        }
        var pattern = Arrays.stream(filter.split(",\\s*"))
            .map(Pattern::quote)
            .map(s -> s.replace("*", "\\E.*\\Q"))
            .collect(Collectors.joining(")|(", "(", ")"));
        return Pattern.compile(pattern);
    }

    /**
     * Start the Mock APM server. Just returns empty responses for every incoming export
     *
     * @throws IOException
     */
    public void start() throws IOException {
        if (grpcInstance != null) {
            throw new IllegalStateException("MockApmServer already started");
        }
        grpcInstance = ServerBuilder.forPort(0).addService(new GrpcMetricsService()).addService(new GrpcTraceService()).build().start();
        logger.lifecycle("MockApmServer gRPC (OTLP metrics + traces) started on port " + grpcInstance.getPort());
    }

    public int getGrpcPort() {
        if (grpcInstance == null) {
            throw new IllegalStateException("MockApmServer not started");
        }
        return grpcInstance.getPort();
    }

    /**
     * Stop the server gracefully if possible
     */
    public void stop() {
        if (grpcInstance != null) {
            logger.lifecycle("stopping apm server");
            grpcInstance.shutdownNow();
            grpcInstance = null;
        }
    }

    class GrpcMetricsService extends MetricsServiceGrpc.MetricsServiceImplBase {
        @Override
        public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver) {
            try {
                logOtlpMetrics(request);
            } catch (Exception e) {
                e.printStackTrace();
            }
            responseObserver.onNext(ExportMetricsServiceResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    private void logOtlpMetrics(ExportMetricsServiceRequest metrics) {
        for (var resourceMetrics : metrics.getResourceMetricsList()) {
            var samples = new ArrayList<String>();
            for (var scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                for (var metric : scopeMetrics.getMetricsList()) {
                    String name = metric.getName();
                    if (metricFilter != null && metricFilter.matcher(name).matches() == false) {
                        continue;
                    }
                    samples.add(metric.toString());
                }
            }
            if (samples.isEmpty() == false) {
                logger.lifecycle("OTLP Metricset:\n{}", String.join("\n", samples));
            }
        }
    }

    class GrpcTraceService extends TraceServiceGrpc.TraceServiceImplBase {
        @Override
        public void export(ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> responseObserver) {
            logger.lifecycle("OTLP Spans:\n{}", request);
            responseObserver.onNext(ExportTraceServiceResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
