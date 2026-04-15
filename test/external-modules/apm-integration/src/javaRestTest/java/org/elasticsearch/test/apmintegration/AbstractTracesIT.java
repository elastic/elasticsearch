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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Abstract base for integration tests that verify trace/span export behaviour.
 * Concrete subclasses provide the cluster (APM agent path or OTel SDK path) and
 * the {@code @ClassRule} that starts both the recording server and the cluster.
 *
 * Both paths should produce the same observable behaviour:
 * <ol>
 *   <li>Root spans are exported with correct W3C traceparent propagation.</li>
 *   <li>Only root (entry-point) spans are exported; child spans are dropped.</li>
 * </ol>
 */
public abstract class AbstractTracesIT extends ESRestTestCase {
    private static final Logger logger = LogManager.getLogger(AbstractTracesIT.class);

    /**
     * The APM agent is reconfigured dynamically after booting and only reloads its configuration
     * every 30 seconds. Give telemetry a good long time before giving up.
     */
    static final int TELEMETRY_TIMEOUT = 40;

    /**
     * After the root-span latch fires, wait briefly before asserting the span count.
     * This gives the export pipeline time to deliver any child spans that should not
     * have been exported — if they are going to leak through, they should arrive within
     * this window. Without this pause, the assertion could pass before a misbehaving
     * exporter has had a chance to send them.
     */
    static final long CHILD_SPAN_GRACE_PERIOD_MS = 500;

    protected static RecordingApmServer recordingApmServer = new RecordingApmServer();

    /**
     * Returns a cluster builder with settings common to all traces integration tests:
     * INTEG_TEST distribution, the {@code apm} and {@code test-apm-integration} modules,
     * tracing enabled, and metrics disabled (to reduce noise).
     */
    protected static LocalClusterSpecBuilder<ElasticsearchCluster> baseTracesClusterBuilder() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.INTEG_TEST)
            .module("test-apm-integration")
            .module("apm")
            .setting("telemetry.tracing.enabled", "true")
            .setting("telemetry.metrics.enabled", "false");
    }

    /**
     * Builds the {@code @ClassRule} rule chain for a subclass:
     * recording server starts first, then cluster, then {@code closeClients()} in finally.
     */
    protected static TestRule buildTracesRuleChain(RecordingApmServer server, ElasticsearchCluster cluster) {
        return RuleChain.outerRule(server).around(cluster).around((base, description) -> new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } finally {
                    try {
                        closeClients();
                    } catch (IOException e) {
                        logger.error("failed to close REST clients after test", e);
                    }
                }
            }
        });
    }

    /**
     * Sends a request with a W3C {@code traceparent} header and asserts that the
     * corresponding root span is exported with the correct trace ID and remote parent span ID.
     *
     * This test also verifies that the span name matches the HTTP route pattern used by ES,
     * and delegates attribute-level assertions to {@link #assertNodeStatsRootSpanAttributes}
     * so that each concrete runner can verify the same semantic data under its own attribute keys.
     */
    public void testRestRootSpanWithTraceParent() throws Exception {
        final String traceIdValue = "0af7651916cd43dd8448eb211c80319c";
        final String remoteParentSpanId = "b7ad6b7169203331";
        final String traceParentValue = "00-" + traceIdValue + "-" + remoteParentSpanId + "-01";

        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<ReceivedTelemetry.ReceivedSpan> rootSpanRef = new AtomicReference<>();

        // Filter only on name + traceId. parentSpanId is checked as an explicit assertion below so
        // that a propagation bug produces an immediate failure message rather than a 40-second timeout.
        Consumer<ReceivedTelemetry> messageConsumer = msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedSpan s
                && "GET /_nodes/stats".equals(s.name())
                && traceIdValue.equals(s.traceId())) {
                logger.info("Root span received: {}", s);
                rootSpanRef.set(s);
                finished.countDown();
            }
        };

        recordingApmServer.addMessageConsumer(messageConsumer);

        Request nodeStatsRequest = new Request("GET", "/_nodes/stats");
        nodeStatsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(Task.TRACE_PARENT_HTTP_HEADER, traceParentValue).build());
        client().performRequest(nodeStatsRequest);
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue(
            "GET /_nodes/stats span with traceId " + traceIdValue + " should be received within timeout",
            finished.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
        );
        ReceivedTelemetry.ReceivedSpan rootSpan = rootSpanRef.get();
        assertTrue("Root span should carry a parent span ID propagated from the traceparent header", rootSpan.parentSpanId().isPresent());
        assertEquals(
            "Root span parent span ID should match the remote parent from the traceparent header",
            remoteParentSpanId,
            rootSpan.parentSpanId().get()
        );
        assertNodeStatsRootSpanAttributes(rootSpan);
    }

    /**
     * Asserts that {@code span} carries the semantic metadata expected of a sampled
     * {@code GET /_nodes/stats} HTTP server span.
     *
     * <p>All concrete subclasses must satisfy these assertions regardless of which export path
     * is active. Attribute keys are normalised to the {@code otel.attributes.*} namespace so
     * that a downstream consumer sees identical keys from every exporter implementation.
     */
    protected void assertNodeStatsRootSpanAttributes(ReceivedTelemetry.ReceivedSpan span) {
        Map<String, Object> attrs = span.attributes();
        // Span kind must be SERVER — distinguishes inbound HTTP requests from outbound client calls.
        assertThat("span kind", attrs.get("otel.span_kind"), is("SERVER"));
        // HTTP semantics
        assertThat("HTTP method", attrs.get("otel.attributes.http.method"), is("GET"));
        assertThat("HTTP status code", attrs.get("otel.attributes.http.status_code"), instanceOf(Number.class));
        assertThat(
            "HTTP status code value",
            ((Number) attrs.get("otel.attributes.http.status_code")).intValue(),
            greaterThanOrEqualTo(200)
        );
        assertThat("HTTP URL", attrs.get("otel.attributes.http.url").toString(), is("/_nodes/stats"));
        assertThat("HTTP flavour", attrs.get("otel.attributes.http.flavour").toString(), not(emptyOrNullString()));
        // ES resource attributes
        assertThat("ES node name", attrs.get("otel.attributes.es.node.name").toString(), not(emptyOrNullString()));
        assertThat("ES cluster name", attrs.get("otel.attributes.es.cluster.name").toString(), not(emptyOrNullString()));
    }

    /**
     * Verifies that only the root (entry-point) span is exported and no child spans leak through.
     *
     * On the APM agent path this is enforced by {@code transaction_max_spans=0} (configured in
     * {@code APMJvmOptions.CONFIG_DEFAULTS}). On the OTel SDK path it must be enforced by ES code.
     */
    public void testOnlyRootSpansExported() throws Exception {
        final String traceIdValue = "1234567890abcdef1234567890abcdef";
        final String traceParentValue = "00-" + traceIdValue + "-abcdef1234567890-01";

        CountDownLatch rootSpanReceived = new CountDownLatch(1);
        // CopyOnWriteArrayList is required: the consumer thread may add spans at any time, including during
        // the grace period after the latch fires, while the main thread reads the count after the sleep.
        List<ReceivedTelemetry.ReceivedSpan> receivedSpans = new CopyOnWriteArrayList<>();

        Consumer<ReceivedTelemetry> messageConsumer = msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedSpan s && traceIdValue.equals(s.traceId())) {
                receivedSpans.add(s);
                if ("GET /_nodes/stats".equals(s.name())) {
                    rootSpanReceived.countDown();
                }
            }
        };

        recordingApmServer.addMessageConsumer(messageConsumer);

        Request nodeStatsRequest = new Request("GET", "/_nodes/stats");
        nodeStatsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(Task.TRACE_PARENT_HTTP_HEADER, traceParentValue).build());
        client().performRequest(nodeStatsRequest);
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue("Root span should be received within timeout", rootSpanReceived.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS));

        Thread.sleep(CHILD_SPAN_GRACE_PERIOD_MS);
        // CopyOnWriteArrayList.add() does a volatile write and size() does a volatile read, so child spans
        // that arrive during the grace period above are guaranteed to be visible here.
        assertEquals("Only the root span should be exported; received: " + receivedSpans, 1, receivedSpans.size());
    }
}
