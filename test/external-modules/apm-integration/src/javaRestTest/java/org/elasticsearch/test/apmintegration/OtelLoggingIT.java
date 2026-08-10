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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.HexFormat;

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

    @Before
    void checkFIPS() {
        assumeFalse("Disabled for FIPS mode: https://github.com/elastic/elasticsearch/issues/154330", inFipsJvm());
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
    }
}
