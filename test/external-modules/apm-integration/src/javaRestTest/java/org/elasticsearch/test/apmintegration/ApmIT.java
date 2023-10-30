/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ApmIT extends ESRestTestCase {
    @ClassRule
    public static RecordingApmServer mockApmServer = new RecordingApmServer();

    @Rule
    public ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.metrics.enabled", "true")
        .setting("tracing.apm.agent.metrics_interval", "1s")
        .setting("tracing.apm.agent.server_url", "http://127.0.0.1:" + mockApmServer.getPort())
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testApmIntegration() throws Exception {
        client().performRequest(new Request("GET", "/_use_apm_metrics"));

        // TestMeterUsages for code that is registering & using metrics
        assertBusy(() -> {
            var usageRecords = mockApmServer.getMessages();
            var maybeUsages = usageRecords.stream().map(APMMessage::new).reduce(APMMessage::merge);
            assertTrue(maybeUsages.isPresent());
            var usages = maybeUsages.get();
            assertThat(usages.getDouble("testDoubleCounter"), closeTo(1.0, 0.001));
            assertThat(usages.getDouble("testLongCounter"), closeTo(1.0, 0.001));
            assertThat(usages.getDouble("testDoubleGauge"), closeTo(1.0, 0.001));
            assertThat(usages.getLong("testLongGauge"), equalTo(1L));
            assertThat(usages.getHistogram("testDoubleHistogram").stream().mapToInt(Bucket::count).sum(), equalTo(2));
            assertThat(usages.getHistogram("testLongHistogram").stream().mapToInt(Bucket::count).sum(), equalTo(2));
        });
    }

    private record Bucket(double value, int count) {}

    private class APMMessage {
        Map<String, List<Bucket>> histograms = new HashMap<>();
        Map<String, Double> doubles = new HashMap<>();
        Map<String, Long> longs = new HashMap<>();

        @SuppressWarnings("unchecked")
        APMMessage(String message) {
            Map<String, Object> parsed;
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, message)) {
                parsed = parser.map();
            } catch (IOException e) {
                fail(e);
                parsed = Collections.emptyMap();
            }
            if ("elasticsearch".equals(getTags(parsed).get("otel_instrumentation_scope_name")) == false) {
                return;
            }
            getSamples(parsed).forEach((name, value) -> {
                if ("histogram".equals(value.get("type"))) {
                    List<Double> values = (List<Double>) value.getOrDefault("values", Collections.emptyList());
                    List<Integer> counts = (List<Integer>) value.getOrDefault("counts", Collections.emptyList());
                    assertThat(values.size(), equalTo(counts.size()));
                    List<Bucket> buckets = new ArrayList<>(values.size());
                    for (int i = 0; i < values.size(); i++) {
                        buckets.add(new Bucket(values.get(i), counts.get(i)));
                    }
                    histograms.put(name, buckets);
                    return;
                }
                if (value.get("value") instanceof Number number) {
                    if (number instanceof Integer intn) {
                        longs.put(name, Long.valueOf(intn));
                    } else if (number instanceof Long longn) {
                        longs.put(name, longn);
                    } else if (number instanceof Float floatn) {
                        doubles.put(name, Double.valueOf(floatn));
                    } else if (number instanceof Double doublen) {
                        doubles.put(name, doublen);
                    } else {
                        fail("unknown number [" + number.getClass().getName() + "] [" + number + "]");
                    }
                }
            });
        }

        @SuppressWarnings("unchecked")
        private static Map<String, String> getTags(Map<String, Object> parsed) {
            return (Map<String, String>) (getMetricset(parsed).getOrDefault("tags", Collections.emptyMap()));
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Map<String, Object>> getSamples(Map<String, Object> parsed) {
            return (Map<String, Map<String, Object>>) getMetricset(parsed).getOrDefault("samples", Collections.emptyMap());
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Map<String, ?>> getMetricset(Map<String, Object> parsed) {
            if (parsed.get("metricset") instanceof Map<?, ?> map) {
                return (Map<String, Map<String, ?>>) map;
            }
            return Collections.emptyMap();
        }

        public List<Bucket> getHistogram(String name) {
            return histograms.get(name);
        }

        public Long getLong(String name) {
            return longs.get(name);
        }

        public Double getDouble(String name) {
            return doubles.get(name);
        }

        public APMMessage merge(APMMessage other) {
            histograms.putAll(other.histograms);
            longs.putAll(other.longs);
            doubles.putAll(other.doubles);
            return this;
        }
    }
}
