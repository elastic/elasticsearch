/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.MutableSystemPropertyProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class MetricsApmIT extends ESRestTestCase {
    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();
    private static final MutableSettingsProvider clusterSettings = new MutableSettingsProvider();
    private static final MutableSystemPropertyProvider systemProperties = new MutableSystemPropertyProvider();
    private final boolean withOTel;

    @ClassRule
    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public MetricsApmIT(@Name("withOTel") boolean withOTel) {
        this.withOTel = withOTel;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.metrics.enabled", "true")
        .settings(clusterSettings)
        .systemProperties(systemProperties)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Restarts the shared test cluster when needed so the parameterized cluster settings and system properties
     * for the current test instance take effect. This follows the same pattern used in {@code AbstractNetty4IT}.
     */
    @Before
    public void maybeRestart() throws IOException {
        String current = systemProperties.get(null).get("telemetry.otel.metrics.enabled");
        if (current == null || current.equals(Boolean.toString(withOTel)) == false) {
            systemProperties.get(null).put("telemetry.otel.metrics.enabled", String.valueOf(withOTel));
            if (withOTel) {
                clusterSettings.get(null).put("telemetry.otel.metrics.interval", "1s");
                clusterSettings.get(null)
                    .put("telemetry.otel.metrics.endpoint", "http://" + recordingApmServer.getHttpAddress() + "/v1/metrics");
            } else {
                clusterSettings.get(null).put("telemetry.agent.metrics_interval", "1s");
                clusterSettings.get(null).put("telemetry.agent.server_url", "http://" + recordingApmServer.getHttpAddress());
            }
            cluster.restart(false);
            closeClients();
            initClient();
        }
    }

    @SuppressWarnings("unchecked")
    public void testApmIntegration() throws Exception {
        Map<String, Predicate<Map<String, Object>>> valueAssertions = new HashMap<>(
            Map.ofEntries(
                assertion("es.test.long_counter.total", m -> ((Number) m.get("value")).doubleValue(), closeTo(1.0, 0.001)),
                assertion("es.test.double_counter.total", m -> ((Number) m.get("value")).doubleValue(), closeTo(1.0, 0.001)),
                assertion("es.test.async_double_counter.total", m -> ((Number) m.get("value")).doubleValue(), closeTo(1.0, 0.001)),
                assertion("es.test.async_long_counter.total", m -> ((Number) m.get("value")).intValue(), equalTo(1)),
                assertion("es.test.double_gauge.current", m -> ((Number) m.get("value")).doubleValue(), closeTo(1.0, 0.001)),
                assertion("es.test.long_gauge.current", m -> ((Number) m.get("value")).intValue(), equalTo(1))
            )
        );

        Map<String, Integer> histogramAssertions = new HashMap<>(
            Map.ofEntries(entry("es.test.double_histogram.histogram", 2), entry("es.test.long_histogram.histogram", 2))
        );

        CountDownLatch finished = new CountDownLatch(1);

        // a consumer that will remove the assertions from a map once it matched
        Consumer<String> messageConsumer = (String message) -> {
            var apmMessage = parseMap(message);
            if (isElasticsearchMetric(apmMessage)) {
                logger.info("Apm metric message received: " + message);

                var metricset = (Map<String, Object>) apmMessage.get("metricset");
                var samples = (Map<String, Object>) metricset.get("samples");

                samples.forEach((key, value) -> {
                    var valueAssertion = valueAssertions.get(key);// sample name
                    if (valueAssertion != null) {
                        logger.info("Matched {}:{}", key, value);
                        var sampleObject = (Map<String, Object>) value;
                        if (valueAssertion.test(sampleObject)) {// sample object
                            logger.info("{} assertion PASSED", key);
                            valueAssertions.remove(key);
                        } else {
                            logger.error("{} assertion FAILED", key);
                        }
                    }
                    var histogramAssertion = histogramAssertions.get(key);
                    if (histogramAssertion != null) {
                        logger.info("Matched {}:{}", key, value);
                        var samplesObject = (Map<String, Object>) value;
                        var counts = ((Collection<? extends Number>) samplesObject.get("counts")).stream().mapToInt(Number::intValue).sum();
                        var remaining = histogramAssertion - counts;
                        if (remaining <= 0) {
                            logger.info("{} assertion PASSED", key);
                            histogramAssertions.remove(key);
                        } else {
                            logger.info("{} assertion PENDING: {} remaining", key, remaining);
                            histogramAssertions.put(key, remaining);
                        }
                    }
                });
            }

            if (valueAssertions.isEmpty() && histogramAssertions.isEmpty()) {
                finished.countDown();
            }
        };

        recordingApmServer.addMessageConsumer(messageConsumer);

        if (withOTel) {
            // Re-trigger periodically to produce fresh non-zero deltas for async counters
            for (int i = 0; i < 15 && finished.getCount() > 0; i++) {
                client().performRequest(new Request("GET", "/_use_apm_metrics"));
                finished.await(2, TimeUnit.SECONDS);
            }
        } else {
            client().performRequest(new Request("GET", "/_use_apm_metrics"));
            finished.await(30, TimeUnit.SECONDS);
        }

        var remainingAssertions = Stream.concat(valueAssertions.keySet().stream(), histogramAssertions.keySet().stream())
            .collect(Collectors.joining(","));
        assertTrue(
            "Timeout when waiting for assertions to complete. Remaining assertions to match: " + remainingAssertions,
            finished.getCount() == 0
        );
    }

    @SuppressWarnings("unchecked")
    public void testJvmMetrics() throws Exception {
        Map<String, Predicate<Map<String, Object>>> valueAssertions = new HashMap<>(
            Map.ofEntries(
                assertion("system.cpu.total.norm.pct", m -> (Double) m.get("value"), closeTo(0.0, 1.0)),
                assertion("system.process.cpu.total.norm.pct", m -> (Double) m.get("value"), closeTo(0.0, 1.0)),
                assertion("system.memory.total", m -> ((Number) m.get("value")).longValue(), greaterThan(0L)),
                assertion("system.memory.actual.free", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("system.process.memory.size", m -> ((Number) m.get("value")).longValue(), greaterThan(0L)),
                assertion("jvm.memory.heap.used", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.memory.heap.committed", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.memory.heap.max", m -> ((Number) m.get("value")).longValue(), greaterThan(0L)),
                assertion("jvm.memory.non_heap.used", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.memory.non_heap.committed", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.gc.count", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.gc.time", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.gc.alloc", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.thread.count", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(1L)),
                assertion("jvm.fd.used", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.fd.max", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.memory.heap.pool.used", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.memory.heap.pool.committed", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.memory.non_heap.pool.used", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L)),
                assertion("jvm.memory.non_heap.pool.committed", m -> ((Number) m.get("value")).longValue(), greaterThanOrEqualTo(0L))
            )
        );

        CountDownLatch finished = new CountDownLatch(1);

        Consumer<String> messageConsumer = (String message) -> {
            var apmMessage = parseMap(message);
            var metricset = (Map<String, Object>) apmMessage.getOrDefault("metricset", Collections.emptyMap());
            var samples = (Map<String, Object>) metricset.getOrDefault("samples", Collections.emptyMap());

            samples.forEach((key, value) -> {
                var valueAssertion = valueAssertions.get(key);
                if (valueAssertion != null) {
                    var sampleObject = (Map<String, Object>) value;
                    if (valueAssertion.test(sampleObject)) {
                        logger.info("{} assertion PASSED", key);
                        valueAssertions.remove(key);
                    }
                }
            });

            if (valueAssertions.isEmpty()) {
                finished.countDown();
            }
        };

        recordingApmServer.addMessageConsumer(messageConsumer);

        var completed = finished.await(30, TimeUnit.SECONDS);
        var remaining = valueAssertions.keySet().stream().collect(Collectors.joining(", "));
        assertTrue("Timeout waiting for JVM metrics. Missing: " + remaining, completed);
    }

    private <T> Map.Entry<String, Predicate<Map<String, Object>>> assertion(
        String sampleKeyName,
        Function<Map<String, Object>, T> accessor,
        Matcher<T> expected
    ) {
        return entry(sampleKeyName, new Predicate<>() {
            @Override
            public boolean test(Map<String, Object> sampleObject) {
                return expected.matches(accessor.apply(sampleObject));
            }

            @Override
            public String toString() {
                StringDescription matcherDescription = new StringDescription();
                expected.describeTo(matcherDescription);
                return sampleKeyName + " " + matcherDescription;
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static boolean isElasticsearchMetric(Map<String, Object> apmMessage) {
        var metricset = (Map<String, Object>) apmMessage.getOrDefault("metricset", Collections.emptyMap());
        var tags = (Map<String, Object>) metricset.getOrDefault("tags", Collections.emptyMap());
        return "elasticsearch".equals(tags.get("otel_instrumentation_scope_name"));
    }

    private Map<String, Object> parseMap(String message) {
        try (XContentParser parser = XCONTENT.XContent().createParser(XContentParserConfiguration.EMPTY, message)) {
            return parser.map();
        } catch (IOException e) {
            fail(e);
            return Collections.emptyMap();
        }
    }

}
