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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
 * Tests in this class are applied to all subclasses, ensuring all tracing implementations satisfy our requirements:
 * <ul>
 *   <li>Root spans are exported with correct W3C traceparent propagation.</li>
 *   <li>Only root (entry-point) spans are exported; child spans are dropped.</li>
 * </ul>
 */
public abstract class AbstractTracesIT extends AbstractTelemetryIT {
    private static final Logger logger = LogManager.getLogger(AbstractTracesIT.class);

    /**
     * After the root-span latch fires, wait briefly before asserting the span count.
     * This gives the export pipeline time to deliver any child spans that should not
     * have been exported — if they are going to leak through, they should arrive within
     * this window. Without this pause, the assertion could pass before a misbehaving
     * exporter has had a chance to send them.
     */
    static final long CHILD_SPAN_GRACE_PERIOD_MS = 500;

    /**
     * Span attribute keys every exporter implementation must produce on the
     * {@code GET /_nodes/stats} root span. Cross-path contract — the upcoming OTel SDK
     * exporter must satisfy each entry. Anything else (e.g. APM-agent-specific HTTP
     * headers, intake-protocol metadata) is permitted by being absent from this set.
     */
    static final Set<String> REQUIRED_NODE_STATS_SPAN_KEYS = Set.of(
        "otel.attributes.es.cluster.name",
        "otel.attributes.es.node.name",
        "otel.attributes.http.flavour",
        "otel.attributes.http.method",
        "otel.attributes.http.status_code",
        "otel.attributes.http.url",
        "otel.span_kind"
    );

    /** Span attribute keys that must never appear on any exporter path. */
    static final Set<String> FORBIDDEN_SPAN_KEYS = Set.of("otel.attributes.http.request.body", "otel.attributes.http.response.body");

    /**
     * Resource attribute keys every exporter implementation must produce. These are the legacy
     * APM-agent metadata keys that downstream consumers depend on today; the OTel SDK swap is
     * meant to be a drop-in replacement, so any future exporter must continue producing them or
     * fail this assertion.
     */
    static final Set<String> REQUIRED_RESOURCE_KEYS = Set.of(
        "service.name",
        "service.version",
        "service.language.name",
        "service.agent.name",
        "service.agent.version"
    );

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

        apmServer().addMessageConsumer(messageConsumer);

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
        assertNodeStatsResourceAttributes();
    }

    /**
     * Asserts that {@code span} carries the semantic metadata expected of a sampled
     * {@code GET /_nodes/stats} HTTP server span.
     *
     * <p>Two layers of assertion:
     * <ol>
     *   <li><b>Value assertions</b> (below) cover the small set of keys where the value — not just
     *       the key's presence — is semantically load-bearing (HTTP method, status code, URL,
     *       span kind).</li>
     *   <li><b>Key-set assertion</b> against {@link #REQUIRED_NODE_STATS_SPAN_KEYS} and
     *       {@link #FORBIDDEN_SPAN_KEYS}. This is the transparency contract every exporter path
     *       must satisfy: every required key present, no forbidden key present.</li>
     * </ol>
     * <p>All concrete subclasses must satisfy both layers. Attribute keys are normalised to the
     * {@code otel.attributes.*} namespace so that a downstream consumer sees identical keys from
     * every exporter implementation.
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

        // Cross-path key-set contract.
        assertContainsAll("nodes_stats span attributes", REQUIRED_NODE_STATS_SPAN_KEYS, attrs.keySet());
        assertContainsNone("nodes_stats span attributes", FORBIDDEN_SPAN_KEYS, attrs.keySet());
    }

    /**
     * Asserts that the resource (telemetry source) that emitted the {@code GET /_nodes/stats} span
     * carries every entry in {@link #REQUIRED_RESOURCE_KEYS}. Locks in the service / sdk attribute
     * set every exporter must produce; the APM-agent path produces these via its
     * {@code metadata} intake event, the OTel SDK path produces them via the Resource on each
     * {@code ResourceSpans} batch.
     *
     * <p>Resource arrives on the first telemetry request from each path; we only need a short wait
     * in case it hasn't arrived yet.
     */
    protected void assertNodeStatsResourceAttributes() throws Exception {
        assertBusy(() -> assertNotNull("no resource event observed yet", apmServer().resource()), 5, TimeUnit.SECONDS);
        ReceivedTelemetry.ReceivedResource resource = apmServer().resource();
        assertContainsAll("nodes_stats resource attributes", REQUIRED_RESOURCE_KEYS, resource.attributes().keySet());
    }

    /** Fail with a sorted list of the required keys missing from {@code observed}. */
    private static void assertContainsAll(String label, Set<String> required, Set<String> observed) {
        Set<String> missing = new TreeSet<>(required);
        missing.removeAll(observed);
        assertTrue(label + " is missing required keys: " + missing, missing.isEmpty());
    }

    /** Fail with a sorted list of the forbidden keys present in {@code observed}. */
    private static void assertContainsNone(String label, Set<String> forbidden, Set<String> observed) {
        Set<String> present = new TreeSet<>(forbidden);
        present.retainAll(observed);
        assertTrue(label + " contains forbidden keys: " + present, present.isEmpty());
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

        apmServer().addMessageConsumer(messageConsumer);

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
