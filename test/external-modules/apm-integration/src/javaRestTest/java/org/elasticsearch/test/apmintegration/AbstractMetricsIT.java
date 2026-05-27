/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.apache.lucene.util.Constants;
import org.elasticsearch.client.Request;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Ensures metrics are being exported as expected.
 * Tests in this class are applied to all subclasses, ensuring all metrics implementations satisfy our requirements.
 */
public abstract class AbstractMetricsIT extends AbstractTelemetryIT {
    private static final Logger logger = LogManager.getLogger(AbstractMetricsIT.class);

    /**
     * Returns a builder with common cluster settings (distribution, modules, telemetry.metrics.enabled).
     * Subclasses add mode-specific settings and call {@code .build()}.
     */
    protected static LocalClusterSpecBuilder<ElasticsearchCluster> baseClusterBuilder() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.INTEG_TEST)
            .module("test-apm-integration")
            .module("apm")
            .setting("telemetry.metrics.enabled", "true");
    }

    public void testExplicitMetrics() throws Exception {
        Map<String, Predicate<Number>> valueAssertions = new HashMap<>(
            Map.ofEntries(
                entry("es.test.long_counter.total", n -> closeTo(1.0, 0.001).matches(n.doubleValue())),
                entry("es.test.double_counter.total", n -> closeTo(1.0, 0.001).matches(n.doubleValue())),
                entry("es.test.async_double_counter.total", n -> closeTo(1.0, 0.001).matches(n.doubleValue())),
                entry("es.test.async_long_counter.total", n -> equalTo(1).matches(n.intValue())),
                entry("es.test.double_gauge.current", n -> closeTo(1.0, 0.001).matches(n.doubleValue())),
                entry("es.test.long_gauge.current", n -> equalTo(1).matches(n.intValue()))
            )
        );

        Map<String, Integer> histogramAssertions = new HashMap<>(
            Map.ofEntries(entry("es.test.double_histogram.histogram", 2), entry("es.test.long_histogram.histogram", 2))
        );

        CountDownLatch finished = new CountDownLatch(1);

        Consumer<ReceivedTelemetry> messageConsumer = (ReceivedTelemetry msg) -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                logger.info("Apm metric message received: {}", m);

                for (Map.Entry<String, ReceivedTelemetry.ReceivedMetricValue> entry : m.samples().entrySet()) {
                    String key = entry.getKey();
                    ReceivedTelemetry.ReceivedMetricValue sampleValue = entry.getValue();

                    var valuePredicate = valueAssertions.get(key);
                    if (valuePredicate != null && sampleValue instanceof ReceivedTelemetry.ValueSample(Number value)) {
                        if (valuePredicate.test(value)) {
                            logger.info("{} assertion PASSED", key);
                            valueAssertions.remove(key);
                        } else {
                            logger.error("{} assertion FAILED", key);
                        }
                    }

                    var histogramExpected = histogramAssertions.get(key);
                    if (histogramExpected != null && sampleValue instanceof ReceivedTelemetry.HistogramSample(var counts)) {
                        int total = counts.stream().mapToInt(Integer::intValue).sum();
                        int remaining = histogramExpected - total;
                        if (remaining == 0) {
                            logger.info("{} assertion PASSED", key);
                            histogramAssertions.remove(key);
                        } else {
                            histogramAssertions.put(key, remaining);
                        }
                    }
                }
            }

            if (valueAssertions.isEmpty() && histogramAssertions.isEmpty()) {
                finished.countDown();
            }
        };

        apmServer().addMessageConsumer(messageConsumer);

        client().performRequest(new Request("GET", "/_use_apm_metrics"));
        client().performRequest(new Request("GET", "/_flush_telemetry"));
        finished.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS);

        var remainingAssertions = Stream.concat(valueAssertions.keySet().stream(), histogramAssertions.keySet().stream())
            .collect(Collectors.joining(","));
        assertTrue(
            "Timeout when waiting for assertions to complete. Remaining assertions to match: " + remainingAssertions,
            finished.getCount() == 0
        );
    }

    public void testJvmMetrics() throws Exception {
        Map<String, Predicate<Number>> valueAssertions = new HashMap<>(
            Map.ofEntries(
                entry("system.cpu.total.norm.pct", n -> closeTo(0.0, 1.0).matches(n.doubleValue())),
                entry("system.process.cpu.total.norm.pct", n -> closeTo(0.0, 1.0).matches(n.doubleValue())),
                entry("system.memory.total", n -> greaterThan(0L).matches(n.longValue())),
                entry("system.memory.actual.free", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("system.process.memory.size", n -> greaterThan(0L).matches(n.longValue())),
                entry("jvm.memory.heap.used", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.memory.heap.committed", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.memory.heap.max", n -> greaterThan(0L).matches(n.longValue())),
                entry("jvm.memory.non_heap.used", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.memory.non_heap.committed", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.gc.count", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.gc.time", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.gc.alloc", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.thread.count", n -> greaterThanOrEqualTo(1L).matches(n.longValue())),
                entry("jvm.fd.used", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.fd.max", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.memory.heap.pool.used", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.memory.heap.pool.committed", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.memory.non_heap.pool.used", n -> greaterThanOrEqualTo(0L).matches(n.longValue())),
                entry("jvm.memory.non_heap.pool.committed", n -> greaterThanOrEqualTo(0L).matches(n.longValue()))
            )
        );

        if (Constants.WINDOWS) {
            // JVM file descriptor metrics are not emitted on Windows.
            valueAssertions.remove("jvm.fd.used");
            valueAssertions.remove("jvm.fd.max");
        }

        CountDownLatch finished = new CountDownLatch(1);

        Consumer<ReceivedTelemetry> messageConsumer = (ReceivedTelemetry msg) -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m) {
                for (Map.Entry<String, ReceivedTelemetry.ReceivedMetricValue> e : m.samples().entrySet()) {
                    String key = e.getKey();
                    var valueAssertion = valueAssertions.get(key);
                    if (valueAssertion != null && e.getValue() instanceof ReceivedTelemetry.ValueSample(Number value)) {
                        if (valueAssertion.test(value)) {
                            logger.info("{} assertion PASSED", key);
                            valueAssertions.remove(key);
                        }
                    }
                }
            }
            if (valueAssertions.isEmpty()) {
                finished.countDown();
            }
        };

        apmServer().addMessageConsumer(messageConsumer);

        client().performRequest(new Request("GET", "/_flush_telemetry"));
        logger.debug("About to wait for telemetry");
        var completed = finished.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS);
        var remaining = valueAssertions.keySet().stream().collect(Collectors.joining(", "));
        assertTrue("Timeout waiting for JVM metrics. Missing: " + remaining, completed);
    }
}
