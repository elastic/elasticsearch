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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
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

public class MetricsApmIT extends ESRestTestCase {

    public RecordingApmServer mockApmServer = new RecordingApmServer();

    public ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.metrics.enabled", "true")
        .setting("telemetry.agent.metrics_interval", "1s")
        .setting("telemetry.agent.server_url", () -> "http://127.0.0.1:" + mockApmServer.getPort())
        .build();

    @Rule
    public TestRule ruleChain = RuleChain.outerRule(mockApmServer)
        .around(cluster)
        .around(
            (base, description) -> new Statement() {
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
            }
        );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testApmIntegration() throws Exception {
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

            if (valueAssertions.isEmpty()) {
                finished.countDown();
            }
        };

        mockApmServer.addMessageConsumer(messageConsumer);

        client().performRequest(new Request("GET", "/_use_apm_metrics"));

        var completed = finished.await(30, TimeUnit.SECONDS);
        var remainingAssertions = Stream.concat(valueAssertions.keySet().stream(), histogramAssertions.keySet().stream())
            .collect(Collectors.joining(","));
        assertTrue("Timeout when waiting for assertions to complete. Remaining assertions to match: " + remainingAssertions, completed);
    }

}
