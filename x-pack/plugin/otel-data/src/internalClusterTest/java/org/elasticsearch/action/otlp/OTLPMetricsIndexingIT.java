/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.action.otlp;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.internal.FailedExportException;
import io.opentelemetry.exporter.internal.grpc.GrpcExporterUtil;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
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
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryPointData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.field.WriteField;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.countedkeyword.CountedKeywordMapperPlugin;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.stack.StackPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class OTLPMetricsIndexingIT extends ESSingleNodeTestCase {

    private static final Resource TEST_RESOURCE = Resource.create(Attributes.of(stringKey("service.name"), "elasticsearch"));
    private static final InstrumentationScopeInfo TEST_SCOPE = InstrumentationScopeInfo.create("io.opentelemetry.example.metrics");
    private OtlpHttpMetricExporter exporter;
    private SdkMeterProvider meterProvider;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(
            DataStreamsPlugin.class,
            InternalSettingsPlugin.class,
            OTelPlugin.class,
            StackPlugin.class,
            EsqlPlugin.class,
            VersionFieldPlugin.class,
            CountedKeywordMapperPlugin.class,
            ConstantKeywordMapperPlugin.class,
            MapperExtrasPlugin.class,
            Wildcard.class,
            IndexLifecycle.class,
            IngestCommonPlugin.class,
            XPackPlugin.class,
            PainlessPlugin.class,
            AnalyticsPlugin.class,
            AggregateMetricMapperPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        // This essentially disables the automatic updates to end_time settings of a data stream's latest backing index.
        newSettings.put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "10m");
        return newSettings.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        exporter = OtlpHttpMetricExporter.builder().setEndpoint("http://localhost:" + getHttpPort() + "/_otlp/v1/metrics").build();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setExecutor(Executors.newScheduledThreadPool(0))
                    .setInterval(Duration.ofNanos(Long.MAX_VALUE))
                    .build()
            )
            .build();
        assertBusy(() -> {
            GetComposableIndexTemplateAction.Request getReq = new GetComposableIndexTemplateAction.Request(TEST_REQUEST_TIMEOUT, "*");
            var templates = client().execute(GetComposableIndexTemplateAction.INSTANCE, getReq).actionGet().indexTemplates();
            assertThat(templates, not(anEmptyMap()));
        });
    }

    private int getHttpPort() {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        assertEquals(1, nodesInfoResponse.getNodes().size());
        NodeInfo node = nodesInfoResponse.getNodes().getFirst();
        assertNotNull(node.getInfo(HttpInfo.class));
        TransportAddress publishAddress = node.getInfo(HttpInfo.class).address().publishAddress();
        InetSocketAddress address = publishAddress.address();
        return address.getPort();
    }

    @Override
    public void tearDown() throws Exception {
        meterProvider.close();
        super.tearDown();
    }

    public void testIngestMetricViaMeterProvider() throws Exception {
        Meter sampleMeter = meterProvider.get("io.opentelemetry.example.metrics");

        sampleMeter.gaugeBuilder("jvm.memory.total")
            .setDescription("Reports JVM memory usage.")
            .setUnit("By")
            .buildWithCallback(result -> result.record(Runtime.getRuntime().totalMemory(), Attributes.empty()));

        var result = meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
        assertThat(result.isSuccess(), is(true));

        admin().indices().prepareRefresh().execute().actionGet();
        String[] indices = admin().indices()
            .prepareGetIndex(TimeValue.timeValueSeconds(1))
            .setIndices("metrics-generic.otel-default")
            .get()
            .indices();
        assertThat(indices, not(emptyArray()));

        try (EsqlQueryResponse resp = query("""
            TS metrics-generic.otel-default
             | STATS avg(jvm.memory.total)
            """)) {
            double avgJvmMemoryTotal = (double) resp.column(0).next();
            assertThat(avgJvmMemoryTotal, greaterThan(0.0));
        }
    }

    public void testIngestMetricDataViaMetricExporter() throws Exception {
        MetricData jvmMemoryMetricData = createDoubleGauge(
            TEST_RESOURCE,
            Attributes.empty(),
            "jvm.memory.total",
            Runtime.getRuntime().totalMemory(),
            "By",
            Clock.getDefault().now()
        );

        export(List.of(jvmMemoryMetricData));
        String[] indices = admin().indices()
            .prepareGetIndex(TimeValue.timeValueSeconds(1))
            .setIndices("metrics-generic.otel-default")
            .get()
            .indices();
        assertThat(indices, not(emptyArray()));

        try (EsqlQueryResponse resp = query("""
            TS metrics-generic.otel-default
             | STATS avg(jvm.memory.total)
            """)) {
            double avgJvmMemoryTotal = (double) resp.column(0).next();
            assertThat(avgJvmMemoryTotal, greaterThan(0.0));
        }
    }

    public void testGroupingSameGroup() throws Exception {
        long now = Clock.getDefault().now();
        MetricData metric1 = createDoubleGauge(TEST_RESOURCE, Attributes.empty(), "metric1", 42, "By", now);
        // uses an equal but not the same resource to test grouping across resourceMetrics
        MetricData metric2 = createDoubleGauge(TEST_RESOURCE.toBuilder().build(), Attributes.empty(), "metric2", 42, "By", now);

        export(List.of(metric1, metric2));

        assertResponse(client().prepareSearch("metrics-generic.otel-default"), resp -> {
            assertThat(resp.getHits().getHits(), arrayWithSize(1));
            Map<String, Object> sourceMap = resp.getHits().getAt(0).getSourceAsMap();
            assertThat(sourceMap.get("metrics"), equalTo(Map.of("metric1", 42.0, "metric2", 42.0)));
            assertThat(sourceMap.get("resource"), equalTo(Map.of("attributes", Map.of("service.name", "elasticsearch"))));
        });
    }

    public void testRoutingSameTimeSeriesDifferentIndex() throws Exception {
        long now = Clock.getDefault().now();
        MetricData metric1 = createDoubleGauge(
            TEST_RESOURCE,
            Attributes.of(stringKey("data_stream.dataset"), "foo"),
            "metric",
            42,
            "By",
            now
        );
        MetricData metric2 = createDoubleGauge(
            TEST_RESOURCE,
            Attributes.of(stringKey("data_stream.dataset"), "bar"),
            "metric",
            42,
            "By",
            now
        );

        export(List.of(metric1, metric2));

        assertResponse(client().prepareSearch("metrics-foo.otel-default"), resp -> {
            assertThat(resp.getHits().getHits(), arrayWithSize(1));
            assertThat(evaluate(resp.getHits().getAt(0).getSourceAsMap(), "metrics.metric"), equalTo(42.0));
        });
        assertResponse(client().prepareSearch("metrics-bar.otel-default"), resp -> {
            assertThat(resp.getHits().getHits(), arrayWithSize(1));
            assertThat(evaluate(resp.getHits().getAt(0).getSourceAsMap(), "metrics.metric"), equalTo(42.0));
        });
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

        assertResponse(
            client().prepareSearch("metrics-generic.otel-default"),
            resp -> assertThat(resp.getHits().getHits(), arrayWithSize(4))
        );
    }

    public void testTimeSeriesMetrics() throws Exception {
        long now = Clock.getDefault().now();
        MetricData metric1 = createCounter(TEST_RESOURCE, Attributes.empty(), "counter", 42, "By", now);
        MetricData metric2 = createDoubleGauge(TEST_RESOURCE, Attributes.empty(), "gauge", 42, "By", now);
        export(List.of(metric1, metric2));

        assertResponse(client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "metrics-generic.otel-default"), resp -> {
            Map<String, MappingMetadata> mappings = resp.getMappings();
            assertThat(mappings, aMapWithSize(1));
            Map<String, Object> mapping = mappings.values().iterator().next().getSourceAsMap();
            assertThat(mapping, not(anEmptyMap()));
            // check that the time_series_metric is present for the created metrics and set correctly
            assertThat(evaluate(mapping, "properties.metrics.properties.counter.type"), equalTo("long"));
            assertThat(evaluate(mapping, "properties.metrics.properties.counter.time_series_metric"), equalTo("counter"));
            assertThat(evaluate(mapping, "properties.metrics.properties.gauge.type"), equalTo("double"));
            assertThat(evaluate(mapping, "properties.metrics.properties.gauge.time_series_metric"), equalTo("gauge"));
        });
    }

    public void testExponentialHistograms() throws Exception {
        long now = Clock.getDefault().now();
        export(List.of(createExponentialHistogram(now, "exponential_histogram", AggregationTemporality.DELTA, Attributes.empty())));

        assertResponse(client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "metrics-generic.otel-default"), resp -> {
            Map<String, MappingMetadata> mappings = resp.getMappings();
            assertThat(mappings, aMapWithSize(1));
            Map<String, Object> mapping = mappings.values().iterator().next().getSourceAsMap();
            assertThat(mapping, not(anEmptyMap()));
            // check that the exponential_histogram is present for the created metrics and set correctly
            assertThat(evaluate(mapping, "properties.metrics.properties.exponential_histogram.type"), equalTo("histogram"));
        });
        // get document and check values/counts array
        assertResponse(client().prepareSearch("metrics-generic.otel-default"), resp -> {
            assertThat(resp.getHits().getHits(), arrayWithSize(1));
            Map<String, Object> sourceMap = resp.getHits().getAt(0).getSourceAsMap();
            assertThat(evaluate(sourceMap, "metrics.exponential_histogram.counts"), equalTo(List.of(3, 2, 1, 10, 1, 2, 3)));
            List<Double> values = evaluate(sourceMap, "metrics.exponential_histogram.values");
            ArrayList<Double> sortedValues = new ArrayList<>(values);
            sortedValues.sort(Double::compareTo);
            assertThat(values, equalTo(sortedValues));
        });
    }

    public void testExponentialHistogramsAsAggregateMetricDouble() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createExponentialHistogram(
                    now,
                    "exponential_histogram_summary",
                    AggregationTemporality.DELTA,
                    Attributes.of(
                        AttributeKey.stringArrayKey("elasticsearch.mapping.hints"),
                        List.of("aggregate_metric_double", "_doc_count")
                    )
                )
            )
        );

        assertResponse(client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "metrics-generic.otel-default"), resp -> {
            Map<String, MappingMetadata> mappings = resp.getMappings();
            assertThat(mappings, aMapWithSize(1));
            Map<String, Object> mapping = mappings.values().iterator().next().getSourceAsMap();
            assertThat(mapping, not(anEmptyMap()));
            assertThat(
                evaluate(mapping, "properties.metrics.properties.exponential_histogram_summary.type"),
                equalTo("aggregate_metric_double")
            );
        });
        assertResponse(client().prepareSearch("metrics-generic.otel-default"), resp -> {
            assertThat(resp.getHits().getHits(), arrayWithSize(1));
            Map<String, Object> sourceMap = resp.getHits().getAt(0).getSourceAsMap();
            assertThat(evaluate(sourceMap, "_doc_count"), equalTo(22));
            assertThat(evaluate(sourceMap, "metrics.exponential_histogram_summary.value_count"), equalTo(22));
            assertThat(evaluate(sourceMap, "metrics.exponential_histogram_summary.sum"), equalTo(10.0));
        });
    }

    public void testHistogram() throws Exception {
        long now = Clock.getDefault().now();
        export(List.of(createHistogram(now, "histogram", AggregationTemporality.DELTA, Attributes.empty())));

        assertResponse(client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "metrics-generic.otel-default"), resp -> {
            Map<String, MappingMetadata> mappings = resp.getMappings();
            assertThat(mappings, aMapWithSize(1));
            Map<String, Object> mapping = mappings.values().iterator().next().getSourceAsMap();
            assertThat(mapping, not(anEmptyMap()));
            // check that the histogram is present for the created metrics and set correctly
            assertThat(evaluate(mapping, "properties.metrics.properties.histogram.type"), equalTo("histogram"));
        });
        // get document and check values/counts array
        assertResponse(client().prepareSearch("metrics-generic.otel-default"), resp -> {
            assertThat(resp.getHits().getHits(), arrayWithSize(1));
            Map<String, Object> sourceMap = resp.getHits().getAt(0).getSourceAsMap();
            assertThat(evaluate(sourceMap, "metrics.histogram.counts"), equalTo(List.of(1, 2, 3, 4, 5, 6)));
            List<Double> values = evaluate(sourceMap, "metrics.histogram.values");
            assertThat(values, equalTo(List.of(1.0, 3.0, 5.0, 7.0, 9.0, 10.0)));
        });
    }

    public void testHistogramAsAggregateMetricDouble() throws Exception {
        long now = Clock.getDefault().now();
        export(
            List.of(
                createHistogram(
                    now,
                    "histogram_summary",
                    AggregationTemporality.DELTA,
                    Attributes.of(
                        AttributeKey.stringArrayKey("elasticsearch.mapping.hints"),
                        List.of("aggregate_metric_double", "_doc_count")
                    )
                )
            )
        );

        assertResponse(client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "metrics-generic.otel-default"), resp -> {
            Map<String, MappingMetadata> mappings = resp.getMappings();
            assertThat(mappings, aMapWithSize(1));
            Map<String, Object> mapping = mappings.values().iterator().next().getSourceAsMap();
            assertThat(mapping, not(anEmptyMap()));
            assertThat(evaluate(mapping, "properties.metrics.properties.histogram_summary.type"), equalTo("aggregate_metric_double"));
        });
        assertResponse(client().prepareSearch("metrics-generic.otel-default"), resp -> {
            assertThat(resp.getHits().getHits(), arrayWithSize(1));
            Map<String, Object> sourceMap = resp.getHits().getAt(0).getSourceAsMap();
            assertThat(evaluate(sourceMap, "_doc_count"), equalTo(21));
            assertThat(evaluate(sourceMap, "metrics.histogram_summary.value_count"), equalTo(21));
            assertThat(evaluate(sourceMap, "metrics.histogram_summary.sum"), equalTo(10.0));
        });
    }

    public void testCumulativeHistograms() {
        long now = Clock.getDefault().now();
        RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> export(
                List.of(
                    createExponentialHistogram(now, "exponential_histogram", AggregationTemporality.CUMULATIVE, Attributes.empty()),
                    createHistogram(now, "histogram", AggregationTemporality.CUMULATIVE, Attributes.empty())
                )
            )
        );
        assertThat(
            exception.getMessage(),
            containsString("cumulative exponential histogram metrics are not supported, ignoring exponential_histogram")
        );
        assertThat(exception.getMessage(), containsString("cumulative histogram metrics are not supported, ignoring histogram"));

        assertThrows(
            IndexNotFoundException.class,
            () -> client().admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices("metrics-generic.otel-default").get()
        );
    }

    public void testSummary() throws Exception {
        long now = Clock.getDefault().now();
        MetricData summaryMetric = ImmutableMetricData.createDoubleSummary(
            TEST_RESOURCE,
            TEST_SCOPE,
            "summary",
            "Summary Test",
            "ms",
            ImmutableSummaryData.create(List.of(ImmutableSummaryPointData.create(now, now, Attributes.empty(), 1, 2.0, List.of())))
        );
        export(List.of(summaryMetric));

        assertResponse(client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "metrics-generic.otel-default"), resp -> {
            Map<String, MappingMetadata> mappings = resp.getMappings();
            assertThat(mappings, aMapWithSize(1));
            Map<String, Object> mapping = mappings.values().iterator().next().getSourceAsMap();
            assertThat(mapping, not(anEmptyMap()));
            // check that the summary is present for the created metrics and set correctly
            assertThat(evaluate(mapping, "properties.metrics.properties.summary.type"), equalTo("aggregate_metric_double"));
        });
        // get document and check sum/value_count
        assertResponse(client().prepareSearch("metrics-generic.otel-default"), resp -> {
            assertThat(resp.getHits().getHits(), arrayWithSize(1));
            Map<String, Object> sourceMap = resp.getHits().getAt(0).getSourceAsMap();
            assertThat(evaluate(sourceMap, "metrics.summary.sum"), equalTo(2.0));
            assertThat(evaluate(sourceMap, "metrics.summary.value_count"), equalTo(1));
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T evaluate(Map<String, Object> map, String path) {
        return (T) new WriteField(path, () -> map).get(null);
    }

    private void export(List<MetricData> metrics) throws IOException {
        var result = exporter.export(metrics).join(10, TimeUnit.SECONDS);
        Throwable failure = result.getFailureThrowable();
        if (failure instanceof FailedExportException.HttpExportException httpExportException) {
            throw new RuntimeException(GrpcExporterUtil.getStatusMessage(httpExportException.getResponse().responseBody()));
        } else if (failure != null) {
            throw new RuntimeException("Failed to export metrics", failure);
        }
        assertThat(result.isSuccess(), is(true));
        admin().indices().prepareRefresh().execute().actionGet();
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

    private static MetricData createCounter(
        Resource resource,
        Attributes attributes,
        String name,
        long value,
        String unit,
        long timeEpochNanos
    ) {
        return ImmutableMetricData.createLongSum(
            resource,
            TEST_SCOPE,
            name,
            "Your description could be here.",
            unit,
            ImmutableSumData.create(
                true,
                AggregationTemporality.CUMULATIVE,
                List.of(ImmutableLongPointData.create(timeEpochNanos, timeEpochNanos, attributes, value))
            )
        );
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
                        false,
                        0,
                        false,
                        0,
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
                        false,
                        0,
                        false,
                        0,
                        ExponentialHistogramBuckets.create(0, 0, List.of(1L, 2L, 3L)),
                        ExponentialHistogramBuckets.create(0, 0, List.of(1L, 2L, 3L)),
                        timeEpochNanos,
                        timeEpochNanos,
                        attributes,
                        List.of()
                    )
                )
            )
        );
    }

    protected EsqlQueryResponse query(String esql) {
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(client()).query(esql).execute().actionGet();
    }
}
