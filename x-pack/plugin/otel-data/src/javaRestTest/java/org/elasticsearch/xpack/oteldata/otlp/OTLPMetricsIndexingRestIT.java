/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramBuckets;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableExponentialHistogramData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.opentelemetry.sdk.metrics.data.AggregationTemporality.CUMULATIVE;
import static io.opentelemetry.sdk.metrics.data.AggregationTemporality.DELTA;
import static org.elasticsearch.test.rest.ObjectPath.evaluate;
import static org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsIndexingRestIT.Monotonicity.MONOTONIC;
import static org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsIndexingRestIT.Monotonicity.NON_MONOTONIC;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;

public class OTLPMetricsIndexingRestIT extends AbstractOTLPIndexingRestIT {

    private OtlpHttpMetricExporter exporter;
    private SdkMeterProvider meterProvider;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        exporter = OtlpHttpMetricExporter.builder()
            .setEndpoint(getClusterHosts().getFirst().toURI() + "/_otlp/v1/metrics")
            .addHeader("Authorization", "ApiKey " + createApiKey("metrics-*"))
            .build();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setExecutor(Executors.newScheduledThreadPool(0))
                    .setInterval(Duration.ofNanos(Long.MAX_VALUE))
                    .build()
            )
            .build();
    }

    /**
     * Sets the xpack.otel_data.histogram_field_type cluster setting to the provided value.
     */
    private static void setHistogramFieldTypeClusterSetting(String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("""
            {
              "persistent": {
                "xpack.otel_data.histogram_field_type": "$setting"
              }
            }
            """.replace("$setting", value));
        assertOK(client().performRequest(request));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        meterProvider.close();
        super.tearDown();
    }

    public void testIngestMetricViaMeterProvider() throws Exception {
        Meter sampleMeter = meterProvider.get("io.opentelemetry.example.metrics");
        long totalMemory = 42;

        sampleMeter.gaugeBuilder("jvm.memory.total")
            .setDescription("Reports JVM memory usage.")
            .setUnit("By")
            .buildWithCallback(result -> result.record(totalMemory, Attributes.empty()));

        var result = meterProvider.shutdown();
        assertThat(result.isSuccess(), is(true));

        refreshMetricsIndices();

        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.toString(), search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(evaluate(source, "@timestamp"), isA(String.class));
        assertThat(evaluate(source, "start_timestamp"), isA(String.class));
        assertThat(evaluate(source, "_metric_names_hash"), isA(String.class));
        assertThat(ObjectPath.<Number>evaluate(source, "metrics.jvm\\.memory\\.total").longValue(), equalTo(totalMemory));
        assertThat(evaluate(source, "unit"), equalTo("By"));
        assertThat(evaluate(source, "scope.name"), equalTo("io.opentelemetry.example.metrics"));
    }

    public void testIngestMetricDataViaMetricExporter() throws Exception {
        long now = Clock.getDefault().now();
        long totalMemory = 42;
        MetricData jvmMemoryMetricData = createLongGauge(TEST_RESOURCE, Attributes.empty(), "jvm.memory.total", totalMemory, "By", now);

        export(List.of(jvmMemoryMetricData));
        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.toString(), search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(Instant.parse(evaluate(source, "@timestamp")), equalTo(Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(now))));
        assertThat(Instant.parse(evaluate(source, "start_timestamp")), equalTo(Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(now))));
        assertThat(evaluate(source, "_metric_names_hash"), isA(String.class));
        assertThat(ObjectPath.<Number>evaluate(source, "metrics.jvm\\.memory\\.total").longValue(), equalTo(totalMemory));
        assertThat(evaluate(source, "unit"), equalTo("By"));
        assertThat(evaluate(source, "resource.attributes.service\\.name"), equalTo("elasticsearch"));
        assertThat(evaluate(source, "scope.name"), equalTo("io.opentelemetry.example.metrics"));
    }

    public void testGroupingSameGroup() throws Exception {
        long now = Clock.getDefault().now();
        MetricData metric1 = createDoubleGauge(TEST_RESOURCE, Attributes.empty(), "metric1", 42, "By", now);
        // uses an equal but not the same resource to test grouping across resourceMetrics
        MetricData metric2 = createDoubleGauge(TEST_RESOURCE.toBuilder().build(), Attributes.empty(), "metric2", 42, "By", now);

        export(List.of(metric1, metric2));

        ObjectPath path = ObjectPath.createFromResponse(
            client().performRequest(new Request("GET", "metrics-generic.otel-default/_search"))
        );
        assertThat(path.toString(), path.evaluate("hits.total.value"), equalTo(1));
        assertThat(path.evaluate("hits.hits.0._source.metrics"), equalTo(Map.of("metric1", 42.0, "metric2", 42.0)));
        assertThat(path.evaluate("hits.hits.0._source.resource"), equalTo(Map.of("attributes", Map.of("service.name", "elasticsearch"))));
    }

    public void testGroupingDifferentGroup() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createDoubleGauge(TEST_RESOURCE, Attributes.empty(), "metric1", 42, "By", now),
                createDoubleGauge(TEST_RESOURCE, Attributes.empty(), "metric1", 42, "By", now + TimeUnit.MILLISECONDS.toNanos(1)),
                createDoubleGauge(TEST_RESOURCE, Attributes.empty(), "metric1", 42, "", now),
                createDoubleGauge(TEST_RESOURCE, Attributes.of(stringKey("foo"), "bar"), "metric1", 42, "By", now)
            )
        );
        ObjectPath path = search("metrics-generic.otel-default");
        assertThat(path.toString(), path.evaluate("hits.total.value"), equalTo(4));
    }

    public void testGauge() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createDoubleGauge(TEST_RESOURCE, Attributes.empty(), "double_gauge", 42.0, "By", now),
                createLongGauge(TEST_RESOURCE, Attributes.empty(), "long_gauge", 42, "By", now)
            )
        );
        Map<String, Object> metrics = evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(evaluate(metrics, "double_gauge.type"), equalTo("double"));
        assertThat(evaluate(metrics, "double_gauge.meta.unit"), equalTo("By"));
        assertThat(evaluate(metrics, "double_gauge.time_series_metric"), equalTo("gauge"));
        assertThat(evaluate(metrics, "long_gauge.type"), equalTo("long"));
        assertThat(evaluate(metrics, "long_gauge.meta.unit"), equalTo("By"));
        assertThat(evaluate(metrics, "long_gauge.time_series_metric"), equalTo("gauge"));
    }

    public void testCounterTemporality() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createCounter(TEST_RESOURCE, Attributes.empty(), "cumulative_counter", 42, "By", now, CUMULATIVE, MONOTONIC),
                createCounter(TEST_RESOURCE, Attributes.empty(), "delta_counter", 42, "By", now, DELTA, MONOTONIC)
            )
        );

        Map<String, Object> metrics = evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(evaluate(metrics, "cumulative_counter.type"), equalTo("long"));
        assertThat(evaluate(metrics, "cumulative_counter.time_series_metric"), equalTo("counter"));
        assertThat(evaluate(metrics, "delta_counter.type"), equalTo("long"));
        assertThat(evaluate(metrics, "delta_counter.time_series_metric"), equalTo("gauge"));
    }

    public void testCounterMonotonicity() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createCounter(TEST_RESOURCE, Attributes.empty(), "up_down_counter", 42, "By", now, CUMULATIVE, NON_MONOTONIC),
                createCounter(TEST_RESOURCE, Attributes.empty(), "up_down_counter_delta", 42, "By", now, DELTA, NON_MONOTONIC)

            )
        );

        Map<String, Object> metrics = evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(evaluate(metrics, "up_down_counter.type"), equalTo("long"));
        assertThat(evaluate(metrics, "up_down_counter.time_series_metric"), equalTo("gauge"));
        assertThat(evaluate(metrics, "up_down_counter_delta.type"), equalTo("long"));
        assertThat(evaluate(metrics, "up_down_counter_delta.time_series_metric"), equalTo("gauge"));
    }

    public void testExponentialHistogramsAsTDigest() throws Exception {
        long now = Clock.getDefault().now();
        export(List.of(createExponentialHistogram(now, "exponential_histogram", DELTA, Attributes.empty())));

        Map<String, Object> mappings = evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(evaluate(mappings, "exponential_histogram.type"), equalTo("histogram"));
        assertThat(evaluate(mappings, "exponential_histogram.time_series_metric"), equalTo("histogram"));

        // Get document and check values/counts array
        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.toString(), search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(evaluate(source, "metrics.exponential_histogram.counts"), equalTo(List.of(2, 1, 10, 1, 2)));
        assertThat(evaluate(source, "metrics.exponential_histogram.values"), equalTo(List.of(-3.0, -1.5, 0.0, 1.5, 3.0)));
    }

    public void testExponentialHistogramsAsExponentialHistogram() throws Exception {
        setHistogramFieldTypeClusterSetting("exponential_histogram");

        long now = Clock.getDefault().now();
        export(List.of(createExponentialHistogram(now, "exponential_histogram", DELTA, Attributes.empty())));

        Map<String, Object> mappings = evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(evaluate(mappings, "exponential_histogram.type"), equalTo("exponential_histogram"));
        assertThat(evaluate(mappings, "exponential_histogram.time_series_metric"), equalTo("histogram"));

        // Get document and check values/counts array
        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.toString(), search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(evaluate(source, "metrics.exponential_histogram.scale"), equalTo(0));
        assertThat(evaluate(source, "metrics.exponential_histogram.zero.count"), equalTo(10));
        assertThat(evaluate(source, "metrics.exponential_histogram.positive.indices"), equalTo(List.of(0, 1)));
        assertThat(evaluate(source, "metrics.exponential_histogram.positive.counts"), equalTo(List.of(1, 2)));
        assertThat(evaluate(source, "metrics.exponential_histogram.negative.indices"), equalTo(List.of(0, 1)));
        assertThat(evaluate(source, "metrics.exponential_histogram.negative.counts"), equalTo(List.of(1, 2)));
        assertThat(evaluate(source, "metrics.exponential_histogram.sum"), equalTo(10.0));
        assertThat(evaluate(source, "metrics.exponential_histogram.min"), equalTo(-2.5));
        assertThat(evaluate(source, "metrics.exponential_histogram.max"), equalTo(2.5));
    }

    public void testExponentialHistogramsAsAggregateMetricDouble() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createExponentialHistogram(
                    now,
                    "exponential_histogram_summary",
                    DELTA,
                    Attributes.of(
                        AttributeKey.stringArrayKey("elasticsearch.mapping.hints"),
                        List.of("aggregate_metric_double", "_doc_count")
                    )
                )
            )
        );

        Map<String, Object> mappings = evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(evaluate(mappings, "exponential_histogram_summary.type"), equalTo("aggregate_metric_double"));

        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.toString(), search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(evaluate(source, "_doc_count"), equalTo(16));
        assertThat(evaluate(source, "metrics.exponential_histogram_summary.value_count"), equalTo(16));
        assertThat(evaluate(source, "metrics.exponential_histogram_summary.sum"), equalTo(10.0));
    }

    public void testHistogramAsTDigest() throws Exception {
        long now = Clock.getDefault().now();
        export(List.of(createHistogram(now, "histogram", DELTA, Attributes.empty())));

        Map<String, Object> mappings = evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(evaluate(mappings, "histogram.type"), equalTo("histogram"));
        assertThat(evaluate(mappings, "histogram.time_series_metric"), equalTo("histogram"));

        // Get document and check values/counts array
        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.toString(), search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(evaluate(source, "metrics.histogram.counts"), equalTo(List.of(1, 2, 3, 4, 5, 6)));
        List<Double> values = evaluate(source, "metrics.histogram.values");
        assertThat(values, equalTo(List.of(1.0, 3.0, 5.0, 7.0, 9.0, 10.0)));
    }

    public void testHistogramsAsExponentialHistogram() throws Exception {
        setHistogramFieldTypeClusterSetting("exponential_histogram");

        long now = Clock.getDefault().now();
        export(List.of(createHistogram(now, "histogram", DELTA, Attributes.empty())));

        Map<String, Object> mappings = evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(evaluate(mappings, "histogram.type"), equalTo("exponential_histogram"));
        assertThat(evaluate(mappings, "histogram.time_series_metric"), equalTo("histogram"));

        // Get document and check values/counts array
        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.toString(), search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(evaluate(source, "metrics.histogram.scale"), equalTo(38)); // ExponentialHistogram.MAX_SCALE
        assertThat(evaluate(source, "metrics.histogram.zero"), equalTo(null));
        assertThat(
            evaluate(source, "metrics.histogram.positive.indices"),
            equalTo(List.of(-274877906945L, 435671174782L, 638246734797L, 771679845024L, 871342349565L, 913124641741L, 1234711177419L))
        );
        assertThat(evaluate(source, "metrics.histogram.positive.counts"), equalTo(List.of(1, 2, 3, 4, 5, 5, 1)));
        assertThat(evaluate(source, "metrics.histogram.negative"), equalTo(null));
        assertThat(evaluate(source, "metrics.histogram.sum"), equalTo(10.0));
        assertThat(evaluate(source, "metrics.histogram.min"), equalTo(0.5));
        assertThat(evaluate(source, "metrics.histogram.max"), equalTo(22.5));
    }

    public void testHistogramAsAggregateMetricDouble() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createHistogram(
                    now,
                    "histogram_summary",
                    DELTA,
                    Attributes.of(
                        AttributeKey.stringArrayKey("elasticsearch.mapping.hints"),
                        List.of("aggregate_metric_double", "_doc_count")
                    )
                )
            )
        );

        Map<String, Object> metrics = evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(evaluate(metrics, "histogram_summary.type"), equalTo("aggregate_metric_double"));

        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.toString(), search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(evaluate(source, "_doc_count"), equalTo(21));
        assertThat(evaluate(source, "metrics.histogram_summary.value_count"), equalTo(21));
        assertThat(evaluate(source, "metrics.histogram_summary.sum"), equalTo(10.0));
    }

    public void testTsidForBulkIsSame() throws Exception {
        // This test is to ensure that the _tsid is the same when indexing via a bulk request or OTLP.
        long now = Clock.getDefault().now();

        export(
            List.of(
                createDoubleGauge(
                    TEST_RESOURCE,
                    Attributes.builder()
                        .put("string", "foo")
                        .put("string_array", "foo", "bar", "baz")
                        .put("boolean", true)
                        .put("long", 42L)
                        .put("double", 42.0)
                        .put("host.ip", "127.0.0.1")
                        .build(),
                    "metric",
                    42,
                    "By",
                    now
                )
            )
        );
        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);
        hasher.addString("metric");
        String metricNamesHash = Long.toHexString(hasher.digestHash().hashCode());
        // Index the same metric via a bulk request
        Request bulkRequest = new Request("POST", "metrics-generic.otel-default/_bulk?refresh");
        bulkRequest.setJsonEntity(
            "{\"create\":{}}\n"
                + """
                    {
                        "@timestamp": $time,
                        "start_timestamp": $time,
                        "data_stream": {
                            "type": "metrics",
                            "dataset": "generic.otel",
                            "namespace": "default"
                        },
                        "_metric_names_hash": "$metric_names_hash",
                        "metrics": {
                            "metric": 42.0
                        },
                        "attributes": {
                            "string": "foo",
                            "string_array": ["foo", "bar", "baz"],
                            "boolean": true,
                            "long": 42,
                            "double": 42.0,
                            "host.ip": "127.0.0.1"
                        },
                        "resource": {
                            "attributes": {
                                "service.name": "elasticsearch"
                            }
                        },
                        "scope": {
                            "name": "io.opentelemetry.example.metrics"
                        },
                        "unit": "By"
                    }
                    """.replace("\n", "")
                    .replace("$time", Long.toString(TimeUnit.NANOSECONDS.toMillis(now) + 1))
                    .replace("$metric_names_hash", metricNamesHash)
                + "\n"
        );
        assertThat(ObjectPath.createFromResponse(client().performRequest(bulkRequest)).evaluate("errors"), equalTo(false));

        ObjectPath searchResponse = ObjectPath.createFromResponse(
            client().performRequest(new Request("GET", "metrics-generic.otel-default/_search?docvalue_fields=_tsid"))
        );
        assertThat(searchResponse.evaluate("hits.total.value"), equalTo(2));
        assertThat(searchResponse.evaluate("hits.hits.0.fields._tsid"), equalTo(searchResponse.evaluate("hits.hits.1.fields._tsid")));
    }

    private static Map<String, Object> getMapping(String target) throws IOException {
        Map<String, Object> mappings = ObjectPath.createFromResponse(client().performRequest(new Request("GET", target + "/_mapping")))
            .evaluate("");
        assertThat(mappings, aMapWithSize(1));
        Map<String, Object> mapping = evaluate(mappings.values().iterator().next(), "mappings");
        assertThat(mapping, not(anEmptyMap()));
        return mapping;
    }

    private void export(List<MetricData> metrics) throws Exception {
        CompletableResultCode result = exporter.export(metrics).join(10, TimeUnit.SECONDS);
        Throwable failure = result.getFailureThrowable();
        if (failure instanceof Exception e) {
            throw e;
        } else if (failure != null) {
            throw new RuntimeException("Failed to export metrics", failure);
        }
        assertThat(result.isSuccess(), is(true));
        refreshMetricsIndices();
    }

    private ObjectPath search(String target) throws IOException {
        return ObjectPath.createFromResponse(client().performRequest(new Request("GET", target + "/_search")));
    }

    private static void refreshMetricsIndices() throws IOException {
        assertOK(client().performRequest(new Request("GET", "metrics-*/_refresh")));
    }

    private static MetricData createDoubleGauge(
        Resource resource,
        Attributes attributes,
        String name,
        double value,
        String unit,
        long timeEpochNanos
    ) {
        return ImmutableMetricData.createDoubleGauge(
            resource,
            TEST_SCOPE,
            name,
            "Your description could be here.",
            unit,
            ImmutableGaugeData.create(List.of(ImmutableDoublePointData.create(timeEpochNanos, timeEpochNanos, attributes, value)))
        );
    }

    private static MetricData createLongGauge(
        Resource resource,
        Attributes attributes,
        String name,
        long value,
        String unit,
        long timeEpochNanos
    ) {
        return ImmutableMetricData.createLongGauge(
            resource,
            TEST_SCOPE,
            name,
            "Your description could be here.",
            unit,
            ImmutableGaugeData.create(List.of(ImmutableLongPointData.create(timeEpochNanos, timeEpochNanos, attributes, value)))
        );
    }

    private static MetricData createCounter(
        Resource resource,
        Attributes attributes,
        String name,
        long value,
        String unit,
        long timeEpochNanos,
        AggregationTemporality temporality,
        Monotonicity monotonicity
    ) {
        return ImmutableMetricData.createLongSum(
            resource,
            TEST_SCOPE,
            name,
            "Your description could be here.",
            unit,
            ImmutableSumData.create(
                monotonicity.isMonotonic(),
                temporality,
                List.of(ImmutableLongPointData.create(timeEpochNanos, timeEpochNanos, attributes, value))
            )
        );
    }

    // this is just to enhance readability of the createCounter calls (avoid boolean parameter)
    enum Monotonicity {
        MONOTONIC(true),
        NON_MONOTONIC(false);

        private final boolean monotonic;

        Monotonicity(boolean monotonic) {
            this.monotonic = monotonic;
        }

        public boolean isMonotonic() {
            return monotonic;
        }
    }

    private static MetricData createHistogram(long timeEpochNanos, String name, AggregationTemporality temporality, Attributes attributes) {
        return ImmutableMetricData.createDoubleHistogram(
            TEST_RESOURCE,
            TEST_SCOPE,
            name,
            "Histogram Test",
            "ms",
            HistogramData.create(
                temporality,
                List.of(
                    HistogramPointData.create(
                        timeEpochNanos,
                        timeEpochNanos,
                        attributes,
                        10,
                        true,
                        0.5,
                        true,
                        22.5,
                        List.of(2.0, 4.0, 6.0, 8.0, 10.0),
                        List.of(1L, 2L, 3L, 4L, 5L, 6L)
                    )
                )
            )
        );
    }

    private static MetricData createExponentialHistogram(
        long timeEpochNanos,
        String name,
        AggregationTemporality temporality,
        Attributes attributes
    ) {
        return ImmutableMetricData.createExponentialHistogram(
            TEST_RESOURCE,
            TEST_SCOPE,
            name,
            "Exponential Histogram Test",
            "ms",
            ImmutableExponentialHistogramData.create(
                temporality,
                List.of(
                    ImmutableExponentialHistogramPointData.create(
                        0,
                        10,
                        10,
                        true,
                        -2.5,
                        true,
                        2.5,
                        ExponentialHistogramBuckets.create(0, 0, List.of(1L, 2L)),
                        ExponentialHistogramBuckets.create(0, 0, List.of(1L, 2L)),
                        timeEpochNanos,
                        timeEpochNanos,
                        attributes,
                        List.of()
                    )
                )
            )
        );
    }
}
