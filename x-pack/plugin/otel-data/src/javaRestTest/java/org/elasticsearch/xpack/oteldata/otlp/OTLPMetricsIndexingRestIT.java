/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;
import org.junit.ClassRule;

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
import static org.elasticsearch.action.otlp.OTLPMetricsIndexingRestIT.Monotonicity.MONOTONIC;
import static org.elasticsearch.action.otlp.OTLPMetricsIndexingRestIT.Monotonicity.NON_MONOTONIC;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;

public class OTLPMetricsIndexingRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";
    private static final Resource TEST_RESOURCE = Resource.create(Attributes.of(stringKey("service.name"), "elasticsearch"));
    private static final InstrumentationScopeInfo TEST_SCOPE = InstrumentationScopeInfo.create("io.opentelemetry.example.metrics");
    private OtlpHttpMetricExporter exporter;
    private SdkMeterProvider meterProvider;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void beforeTest() throws Exception {
        exporter = OtlpHttpMetricExporter.builder()
            .setEndpoint(getClusterHosts().getFirst().toURI() + "/_otlp/v1/metrics")
            .addHeader("Authorization", basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray())))
            .build();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setExecutor(Executors.newScheduledThreadPool(0))
                    .setInterval(Duration.ofNanos(Long.MAX_VALUE))
                    .build()
            )
            .build();
        assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/metrics-otel@template"))));
        boolean otlpEndpointEnabled = false;
        try {
            otlpEndpointEnabled = RestStatus.isSuccessful(
                client().performRequest(new Request("POST", "/_otlp/v1/metrics")).getStatusLine().getStatusCode()
            );
        } catch (Exception ignore) {}
        assumeTrue("Requires otlp_metrics feature flag to be enabled", otlpEndpointEnabled);
    }

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

        var result = meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
        assertThat(result.isSuccess(), is(true));

        refreshMetricsIndices();

        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(ObjectPath.evaluate(source, "@timestamp"), isA(String.class));
        assertThat(ObjectPath.evaluate(source, "start_timestamp"), isA(String.class));
        assertThat(ObjectPath.evaluate(source, "_metric_names_hash"), isA(String.class));
        assertThat(ObjectPath.<Number>evaluate(source, "metrics.jvm\\.memory\\.total").longValue(), equalTo(totalMemory));
        assertThat(ObjectPath.evaluate(source, "unit"), equalTo("By"));
        assertThat(ObjectPath.evaluate(source, "scope.name"), equalTo("io.opentelemetry.example.metrics"));
    }

    public void testIngestMetricDataViaMetricExporter() throws Exception {
        long now = Clock.getDefault().now();
        long totalMemory = 42;
        MetricData jvmMemoryMetricData = createLongGauge(TEST_RESOURCE, Attributes.empty(), "jvm.memory.total", totalMemory, "By", now);

        export(List.of(jvmMemoryMetricData));
        ObjectPath search = search("metrics-generic.otel-default");
        assertThat(search.evaluate("hits.total.value"), equalTo(1));
        var source = search.evaluate("hits.hits.0._source");
        assertThat(ObjectPath.evaluate(source, "@timestamp"), equalTo(timestampAsString(now)));
        assertThat(ObjectPath.evaluate(source, "start_timestamp"), equalTo(timestampAsString(now)));
        assertThat(ObjectPath.evaluate(source, "_metric_names_hash"), isA(String.class));
        assertThat(ObjectPath.<Number>evaluate(source, "metrics.jvm\\.memory\\.total").longValue(), equalTo(totalMemory));
        assertThat(ObjectPath.evaluate(source, "unit"), equalTo("By"));
        assertThat(ObjectPath.evaluate(source, "resource.attributes.service\\.name"), equalTo("elasticsearch"));
        assertThat(ObjectPath.evaluate(source, "scope.name"), equalTo("io.opentelemetry.example.metrics"));
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
        assertThat(path.evaluate("hits.total.value"), equalTo(1));
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
        assertThat(path.evaluate("hits.total.value"), equalTo(4));
    }

    public void testGauge() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createDoubleGauge(TEST_RESOURCE, Attributes.empty(), "double_gauge", 42.0, "By", now),
                createLongGauge(TEST_RESOURCE, Attributes.empty(), "long_gauge", 42, "By", now)
            )
        );
        Map<String, Object> metrics = ObjectPath.evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(ObjectPath.evaluate(metrics, "double_gauge.type"), equalTo("double"));
        assertThat(ObjectPath.evaluate(metrics, "double_gauge.time_series_metric"), equalTo("gauge"));
        assertThat(ObjectPath.evaluate(metrics, "long_gauge.type"), equalTo("long"));
        assertThat(ObjectPath.evaluate(metrics, "long_gauge.time_series_metric"), equalTo("gauge"));
    }

    public void testCounterTemporality() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createCounter(TEST_RESOURCE, Attributes.empty(), "cumulative_counter", 42, "By", now, CUMULATIVE, MONOTONIC),
                createCounter(TEST_RESOURCE, Attributes.empty(), "delta_counter", 42, "By", now, DELTA, MONOTONIC)
            )
        );

        Map<String, Object> metrics = ObjectPath.evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(ObjectPath.evaluate(metrics, "cumulative_counter.type"), equalTo("long"));
        assertThat(ObjectPath.evaluate(metrics, "cumulative_counter.time_series_metric"), equalTo("counter"));
        assertThat(ObjectPath.evaluate(metrics, "delta_counter.type"), equalTo("long"));
        assertThat(ObjectPath.evaluate(metrics, "delta_counter.time_series_metric"), equalTo("gauge"));
    }

    public void testCounterMonotonicity() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createCounter(TEST_RESOURCE, Attributes.empty(), "up_down_counter", 42, "By", now, CUMULATIVE, NON_MONOTONIC),
                createCounter(TEST_RESOURCE, Attributes.empty(), "up_down_counter_delta", 42, "By", now, DELTA, NON_MONOTONIC)

            )
        );

        Map<String, Object> metrics = ObjectPath.evaluate(getMapping("metrics-generic.otel-default"), "properties.metrics.properties");
        assertThat(ObjectPath.evaluate(metrics, "up_down_counter.type"), equalTo("long"));
        assertThat(ObjectPath.evaluate(metrics, "up_down_counter.time_series_metric"), equalTo("gauge"));
        assertThat(ObjectPath.evaluate(metrics, "up_down_counter_delta.type"), equalTo("long"));
        assertThat(ObjectPath.evaluate(metrics, "up_down_counter_delta.time_series_metric"), equalTo("gauge"));
    }

    private static Map<String, Object> getMapping(String target) throws IOException {
        Map<String, Object> mappings = ObjectPath.createFromResponse(client().performRequest(new Request("GET", target + "/_mapping")))
            .evaluate("");
        assertThat(mappings, aMapWithSize(1));
        Map<String, Object> mapping = ObjectPath.evaluate(mappings.values().iterator().next(), "mappings");
        assertThat(mapping, not(anEmptyMap()));
        return mapping;
    }

    private static String timestampAsString(long now) {
        return Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(now)).toString();
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
}
