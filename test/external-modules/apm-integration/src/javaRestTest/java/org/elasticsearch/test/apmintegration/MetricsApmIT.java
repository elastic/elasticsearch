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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

public class MetricsApmIT extends ESRestTestCase {
    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();

    @ClassRule
    public static RecordingApmServer mockApmServer = new RecordingApmServer();

    @Rule
    public ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.metrics.enabled", "true")
        .setting("telemetry.agent.metrics_interval", "1s")
        .setting("telemetry.agent.server_url", "http://127.0.0.1:" + mockApmServer.getPort())
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    public void testApmIntegration() throws Exception {
        Map<String, Predicate<Map<String, Object>>> valueAssertions = new HashMap<>(
            Map.ofEntries(
                assertion("es.test.long_counter.total", m -> (Double) m.get("value"), closeTo(1.0, 0.001)),
                assertion("es.test.double_counter.total", m -> (Double) m.get("value"), closeTo(1.0, 0.001)),
                assertion("es.test.async_double_counter.total", m -> (Double) m.get("value"), closeTo(1.0, 0.001)),
                assertion("es.test.async_long_counter.total", m -> (Integer) m.get("value"), equalTo(1)),
                assertion("es.test.double_gauge.current", m -> (Double) m.get("value"), closeTo(1.0, 0.001)),
                assertion("es.test.long_gauge.current", m -> (Integer) m.get("value"), equalTo(1))
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
                        var counts = ((Collection<Integer>) samplesObject.get("counts")).stream().mapToInt(Integer::intValue).sum();
                        var remaining = histogramAssertion - counts;
                        if (remaining == 0) {
                            logger.info("{} assertion PASSED", key);
                            histogramAssertions.remove(key);
                        } else {
                            logger.info("{} assertion PENDING: {} remaining", key, remaining);
                            histogramAssertions.put(key, remaining);
                        }
                    }
                });
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
        mockApmServer.stop();
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
