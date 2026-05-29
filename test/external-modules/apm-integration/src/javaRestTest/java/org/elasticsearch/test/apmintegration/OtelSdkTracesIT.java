/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

/**
 * Runs the shared {@link AbstractTracesIT} test suite against the OTel SDK export path.
 *
 * Activated by setting the JVM system property {@code telemetry.otel.traces.enabled=true}.
 * Spans are exported via {@code SdkTracerProvider} + OTLP HTTP, bypassing the Elastic APM
 * Java agent. Child-span filtering and stack-trace suppression are enforced by ES code in
 * {@code APMTracer} when {@code maxChildSpans=0} and {@code stackTraceLimit=0} (the defaults).
 */
public class OtelSdkTracesIT extends AbstractTracesIT {

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = baseTracesClusterBuilder().systemProperty("telemetry.otel.traces.enabled", "true")
        .setting("telemetry.otel.traces.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/traces")
        .build();

    @ClassRule
    public static TestRule ruleChain = buildRuleChain(recordingApmServer, cluster);

    @Override
    protected RecordingApmServer apmServer() {
        return recordingApmServer;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * The SDK path uses {@code SdkTracerProvider.forceFlush()}, which actually flushes
     * buffered spans. 15 s is sufficient; the 40 s default is reserved for the APM agent,
     * which has no programmatic flush API.
     */
    @Override
    protected int telemetryTimeout() {
        return 15;
    }
}
