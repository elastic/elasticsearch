/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import io.opentelemetry.proto.common.v1.ArrayValue;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.otelfilter.TestOtelFilterPlugin;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.tasks.Task.TRACE_PARENT_HTTP_HEADER;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that audit events emitted by {@code LoggingAuditTrail} flow out via the OTel SDK as
 * OTLP log records and arrive at {@link RecordingApmServer} over gRPC.
 *
 * <p>This is the end-to-end pipeline test for ES-14356: log4j → OpenTelemetryAppender (attached
 * programmatically by {@code OtelSdkExportLogsSupplier}) → {@code SdkLoggerProvider} →
 * {@code OtlpGrpcLogRecordExporter} → gRPC recording server.
 */
public class OtelLoggingIT extends AbstractTelemetryIT {

    private static final String API_USER = "api_user";

    public static RecordingApmServer recordingApmServer = RecordingApmServer.withMtls();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.DEFAULT)
        .module("test-apm-integration")
        .module("apm")
        .module("test-otel-filter-plugin")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.security.audit.logfile.events.include", "[ \"_all\" ]")
        // Match the serverless posture: cluster/node identity fields are platform internals and
        // must not appear on records that ship out via OTLP. Production sets these in
        // distribution/archives/src/serverless-default-settings.yml; here we set them on the
        // test cluster so the assertion below covers the same mechanism.
        .setting("xpack.security.audit.logfile.emit_node_name", "false")
        .setting("xpack.security.audit.logfile.emit_node_id", "false")
        .setting("xpack.security.audit.logfile.emit_cluster_name", "false")
        .setting("xpack.security.audit.logfile.emit_cluster_uuid", "false")
        .setting("telemetry.logs.audit.enabled", "true")
        .setting("telemetry.logs.querylog.enabled", "true")
        // OTLP/gRPC endpoint: scheme https for mTLS, no path (different shape than HTTP-protobuf endpoint).
        .setting("telemetry.logs.endpoint", () -> recordingApmServer.getGrpcEndpoint())
        // mTLS: ES node verifies the recording server's cert and presents a client cert.
        // Lambdas are evaluated after recordingApmServer.before() writes the cert files.
        .setting("telemetry.logs.ssl.certificate_authorities", () -> recordingApmServer.getMtlsServerCaCertPath())
        .setting("telemetry.logs.ssl.certificate", () -> recordingApmServer.getMtlsClientCertPath())
        .setting("telemetry.logs.ssl.key", () -> recordingApmServer.getMtlsClientKeyPath())
        .setting("elasticsearch.querylog.enabled", "true")
        .user(API_USER, "api-password", "superuser", false)
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

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(API_USER, new SecureString("api-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testAuditEventArrivesAsOtlpLogRecord() throws Exception {
        ReceivedTelemetry.ReceivedLog log = recordingApmServer.await(
            ReceivedTelemetry.ReceivedLog.class,
            l -> true,
            TELEMETRY_TIMEOUT,
            () -> {
                // Authenticated request — should produce an authentication_success audit event.
                client().performRequest(new Request("GET", "/_security/_authenticate"));
                // Force a flush so the test doesn't race the BatchLogRecordProcessor's schedule.
                client().performRequest(new Request("GET", "/_flush_telemetry"));
            }
        );
        assertNotNull(log);
        assertNotNull(log.attributes());
        assertNotNull("audit log should carry event.action", log.attributes().get("event.action"));
        assertNotNull("audit log should carry event.type", log.attributes().get("event.type"));
        // R6: cluster and node identity fields must not be present on records that ship via OTLP.
        // The four EMIT_*_SETTING gates are off (see cluster setup above), which suppresses the
        // fields at the StringMapMessage source so the OpenTelemetryAppender doesn't capture them.
        assertNull("cluster.name must not be on OTel records", log.attributes().get("cluster.name"));
        assertNull("cluster.uuid must not be on OTel records", log.attributes().get("cluster.uuid"));
        assertNull("node.name must not be on OTel records", log.attributes().get("node.name"));
        assertNull("node.id must not be on OTel records", log.attributes().get("node.id"));
        assertNull("cluster.name must not be on OTel records", log.attributes().get("log4j.map_message.cluster.name"));
        assertNull("cluster.uuid must not be on OTel records", log.attributes().get("log4j.map_message.cluster.uuid"));
        assertNull("node.name must not be on OTel records", log.attributes().get("log4j.map_message.node.name"));
        assertNull("node.id must not be on OTel records", log.attributes().get("log4j.map_message.node.id"));
        assertLogDeliveryResource();
    }

    public void testOtelLoggingOnSearch() throws Exception {
        createIndex("test_index");

        var randomId = HexFormat.of().formatHex(randomByteArrayOfLength(16));
        ReceivedTelemetry.ReceivedLog log = recordingApmServer.await(
            ReceivedTelemetry.ReceivedLog.class,
            l -> l.scopeName().equals(QueryLogging.QUERY_LOGGER_NAME),
            TELEMETRY_TIMEOUT,
            () -> {
                var search = new Request("GET", "/test_index/_search");
                var traceId = "00-" + randomId + "-00f067aa0ba902b7-01";
                RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader(TRACE_PARENT_HTTP_HEADER, traceId).build();
                search.setOptions(options);
                client().performRequest(search);
                // Force a flush so the test doesn't race the BatchLogRecordProcessor's schedule.
                client().performRequest(new Request("GET", "/_flush_telemetry"));
            }
        );
        assertNotNull(log);
        assertNotNull(log.attributes());
        assertThat(log.traceId().get(), equalTo(randomId));
        var indices = (ArrayValue) log.attributes().get(QueryLogging.QUERY_FIELD_INDICES);
        assertThat(indices.getValuesList().getFirst().getStringValue(), equalTo("test_index"));
        assertLogDeliveryResource();
    }

    private void assertLogDeliveryResource() {
        ReceivedTelemetry.ReceivedResource resource = apmServer().logResource();
        assertNotNull("log export should carry a resource", resource);
        assertThat(resource.attributes(), equalTo(Map.of("service.name", "elasticsearch", "service.type", "elasticsearch")));
    }

    /**
     * Verifies that the plugin filter can drop querylog events: searches on
     * {@value TestOtelFilterPlugin#DROP_INDEX_NAME} are suppressed, while a subsequent search
     * on a normal index still flows through (proving the appender is still live).
     */
    public void testQuerylogFilterDropsEvents() throws Exception {
        createIndex(TestOtelFilterPlugin.DROP_INDEX_NAME);
        createIndex("filter_pass_index");

        // Track whether a log for the drop index ever arrives.
        AtomicBoolean dropIndexSeen = new AtomicBoolean(false);
        CountDownLatch passIndexArrived = new CountDownLatch(1);
        AtomicReference<ReceivedTelemetry.ReceivedLog> passLog = new AtomicReference<>();

        Consumer<ReceivedTelemetry> consumer = msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedLog log && log.scopeName().equals(QueryLogging.QUERY_LOGGER_NAME)) {
                Object indicesAttr = log.attributes().get(QueryLogging.QUERY_FIELD_INDICES);
                if (indicesAttr instanceof ArrayValue av
                    && av.getValuesList().stream().anyMatch(v -> TestOtelFilterPlugin.DROP_INDEX_NAME.equals(v.getStringValue()))) {
                    dropIndexSeen.set(true);
                }
                // A log with the marker field came from "filter_pass_index"
                if (TestOtelFilterPlugin.MARKER_VALUE.equals(log.attributes().get(TestOtelFilterPlugin.MARKER_FIELD))) {
                    if (passLog.compareAndSet(null, log)) {
                        passIndexArrived.countDown();
                    }
                }
            }
        };
        recordingApmServer.addMessageConsumer(consumer);

        // Search the drop index first; its querylog event should be suppressed by the filter.
        client().performRequest(new Request("GET", "/" + TestOtelFilterPlugin.DROP_INDEX_NAME + "/_search"));
        // Then search the pass index; its record carries the marker and signals the flush reached the server.
        client().performRequest(new Request("GET", "/filter_pass_index/_search"));
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue("Timeout waiting for pass-index querylog record", passIndexArrived.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS));
        assertFalse("Querylog record for drop index must be suppressed by the filter", dropIndexSeen.get());
    }
}
