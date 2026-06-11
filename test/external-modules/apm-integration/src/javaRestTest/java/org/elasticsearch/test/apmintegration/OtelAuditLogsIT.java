/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Verifies that audit events emitted by {@code LoggingAuditTrail} flow out via the OTel SDK as
 * OTLP log records and arrive at {@link RecordingApmServer} over gRPC.
 *
 * <p>This is the end-to-end pipeline test for ES-14356: log4j → OpenTelemetryAppender (attached
 * programmatically by {@code OtelSdkExportLogsSupplier}) → {@code SdkLoggerProvider} →
 * {@code OtlpGrpcLogRecordExporter} → gRPC recording server.
 */
public class OtelAuditLogsIT extends AbstractTelemetryIT {

    private static final Logger logger = LogManager.getLogger(OtelAuditLogsIT.class);

    private static final String API_USER = "api_user";

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

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
        .setting("telemetry.otel.logs.enabled", "true")
        // OTLP/gRPC endpoint: scheme http, no path (different shape than HTTP-protobuf endpoint).
        .setting("telemetry.otel.logs.endpoint", () -> recordingApmServer.getGrpcEndpoint())
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
        CountDownLatch arrived = new CountDownLatch(1);
        AtomicReference<ReceivedTelemetry.ReceivedLog> firstAuditLog = new AtomicReference<>();

        Consumer<ReceivedTelemetry> consumer = msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedLog log) {
                logger.debug("Received log: body=[{}] attributes={}", log.body(), log.attributes());
                if (firstAuditLog.compareAndSet(null, log)) {
                    arrived.countDown();
                }
            }
        };
        recordingApmServer.addMessageConsumer(consumer);

        // Authenticated request — should produce an authentication_success audit event.
        client().performRequest(new Request("GET", "/_security/_authenticate"));
        // Force a flush so the test doesn't race the BatchLogRecordProcessor's schedule.
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        boolean got = arrived.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS);
        assertTrue("Timeout waiting for an OTLP log record from LoggingAuditTrail", got);
        ReceivedTelemetry.ReceivedLog log = firstAuditLog.get();
        assertNotNull(log);
        assertNotNull(log.attributes());
        // PR 1 intentionally asserts on the log4j.map_message. prefix: the OpenTelemetryAppender
        // captures StringMapMessage entries as prefixed attributes when
        // setCaptureMapMessageAttributes(true) is set. Stripping the prefix is tracked in #4183.
        assertNotNull("audit log should carry event.action", log.attributes().get("log4j.map_message.event.action"));
        assertNotNull("audit log should carry event.type", log.attributes().get("log4j.map_message.event.type"));
        // R6: cluster and node identity fields must not be present on records that ship via OTLP.
        // The four EMIT_*_SETTING gates are off (see cluster setup above), which suppresses the
        // fields at the StringMapMessage source so the OpenTelemetryAppender doesn't capture them.
        assertNull("cluster.name must not be on OTel records", log.attributes().get("log4j.map_message.cluster.name"));
        assertNull("cluster.uuid must not be on OTel records", log.attributes().get("log4j.map_message.cluster.uuid"));
        assertNull("node.name must not be on OTel records", log.attributes().get("log4j.map_message.node.name"));
        assertNull("node.id must not be on OTel records", log.attributes().get("log4j.map_message.node.id"));
    }
}
