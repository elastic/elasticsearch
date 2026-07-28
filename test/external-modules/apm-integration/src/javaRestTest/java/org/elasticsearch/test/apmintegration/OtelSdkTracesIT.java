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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Runs the shared {@link AbstractTracesIT} test suite against the OTel SDK export path.
 *
 * Activated by setting the JVM system property {@code telemetry.otel.traces.enabled=true}.
 * Spans are exported via {@code SdkTracerProvider} + OTLP/gRPC, bypassing the Elastic APM
 * Java agent. Child-span filtering is enforced by ES code in {@code APMTracer} when
 * {@code telemetry.tracing.max_depth=0} (the default). Exception-stack suppression
 * is enforced by the same code when {@code telemetry.tracing.record_exception_stacks=false}
 * (the default); see {@code APMTracerTests} for coverage of that branch.
 */
public class OtelSdkTracesIT extends AbstractTracesIT {

    private static final String EXPECTED_PROJECT_ID = "integ-test-project";
    private static final String EXPECTED_PROJECT_TYPE = "elasticsearch";
    private static final String EXPECTED_NODE_TIER = "index";

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = baseTracesClusterBuilder().systemProperty("telemetry.otel.traces.enabled", "true")
        .setting("telemetry.export.endpoint", () -> recordingApmServer.getGrpcEndpoint())
        .setting("telemetry.tracing.sample_rate", "1.0")
        // Mirrors the three labels ServerlessServerCli writes via telemetry.agent.global_labels.* on the APM-agent path,
        // bridged here to the OTel resource via the telemetry.resource.* affix.
        .setting("telemetry.resource.elasticsearch.project.id", EXPECTED_PROJECT_ID)
        .setting("telemetry.resource.elasticsearch.project.type", EXPECTED_PROJECT_TYPE)
        .setting("telemetry.resource.elasticsearch.node.tier", EXPECTED_NODE_TIER)
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

    /**
     * Extends the base required keys with the OTel HTTP semantic convention attributes
     * produced by {@code APMHttpServerInstrumentation} on the SDK export path.
     */
    @Override
    protected Set<String> requiredNodeStatsSpanKeys() {
        var keys = new HashSet<>(super.requiredNodeStatsSpanKeys());
        keys.addAll(
            Set.of(
                "otel.attributes.http.request.method",
                "otel.attributes.http.response.status_code",
                "otel.attributes.http.route",
                "otel.attributes.network.protocol.version",
                "otel.attributes.url.path"
            )
        );
        return Collections.unmodifiableSet(keys);
    }

    @Override
    protected void assertNodeStatsRootSpanAttributes(ReceivedTelemetry.ReceivedSpan span) {
        super.assertNodeStatsRootSpanAttributes(span);
        Map<String, Object> attrs = span.attributes();
        assertThat("OTel HTTP request method", attrs.get("otel.attributes.http.request.method"), is("GET"));
        assertThat("OTel HTTP route", attrs.get("otel.attributes.http.route").toString(), is("/_nodes/stats"));
        assertThat("OTel URL path", attrs.get("otel.attributes.url.path").toString(), is("/_nodes/stats"));
        assertThat("OTel HTTP response status code", attrs.get("otel.attributes.http.response.status_code"), instanceOf(Number.class));
        assertThat(
            "OTel HTTP response status code value",
            ((Number) attrs.get("otel.attributes.http.response.status_code")).intValue(),
            greaterThanOrEqualTo(200)
        );
        assertThat(
            "OTel network protocol version",
            attrs.get("otel.attributes.network.protocol.version").toString(),
            not(emptyOrNullString())
        );
    }

    public void testResourceCarriesAffix() throws Exception {
        assertSdkResourceAttributes(EXPECTED_PROJECT_ID, EXPECTED_PROJECT_TYPE, EXPECTED_NODE_TIER);
    }
}
