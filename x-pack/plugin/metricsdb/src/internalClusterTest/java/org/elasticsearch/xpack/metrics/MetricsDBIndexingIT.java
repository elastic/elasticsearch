/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class MetricsDBIndexingIT extends ESSingleNodeTestCase {

    private OtlpHttpMetricExporter exporter;
    private SdkMeterProvider meterProvider;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(
            DataStreamsPlugin.class,
            InternalSettingsPlugin.class,
            MetricsDBPlugin.class,
            EsqlPlugin.class,
            VersionFieldPlugin.class
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
            GetComposableIndexTemplateAction.Request getReq = new GetComposableIndexTemplateAction.Request(
                TEST_REQUEST_TIMEOUT,
                "metricsdb@template"
            );
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

    @Test
    public void testIngestMetricViaMeterProvider() throws Exception {
        Meter sampleMeter = meterProvider.get("io.opentelemetry.example.metrics");

        sampleMeter.gaugeBuilder("jvm.memory.total")
            .setDescription("Reports JVM memory usage.")
            .setUnit("By")
            .buildWithCallback(result -> result.record(Runtime.getRuntime().totalMemory(), Attributes.empty()));

        var result = meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
        assertThat(result.isSuccess(), is(true));

        admin().indices().prepareRefresh().execute().actionGet();
        String[] indices = admin().indices().prepareGetIndex(TimeValue.timeValueSeconds(1)).setIndices("metricsdb").get().indices();
        assertThat(indices, not(emptyArray()));

        try (EsqlQueryResponse resp = query("""
            FROM metricsdb
             | STATS avg(metric.value.double) WHERE metric.name == "jvm.memory.total"
            """)) {
            double avgJvmMemoryTotal = (double) resp.column(0).next();
            assertThat(avgJvmMemoryTotal, greaterThan(0.0));
        }
    }

    @Test
    public void testIngestMetricDataViaMetricExporter() throws Exception {
        MetricData jvmMemoryMetricData = ImmutableMetricData.createDoubleGauge(
            Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "elasticsearch")),
            InstrumentationScopeInfo.create("io.opentelemetry.example.metrics"),
            "jvm.memory.total",
            "Reports JVM memory usage.",
            "By",
            ImmutableGaugeData.create(
                List.of(
                    ImmutableDoublePointData.create(
                        Clock.getDefault().now(),
                        Clock.getDefault().now(),
                        Attributes.empty(),
                        Runtime.getRuntime().totalMemory()
                    )
                )
            )
        );

        var result = exporter.export(List.of(jvmMemoryMetricData)).join(10, TimeUnit.SECONDS);
        assertThat(result.isSuccess(), is(true));

        admin().indices().prepareRefresh().execute().actionGet();
        String[] indices = admin().indices().prepareGetIndex(TimeValue.timeValueSeconds(1)).setIndices("metricsdb").get().indices();
        assertThat(indices, not(emptyArray()));

        try (EsqlQueryResponse resp = query("""
            FROM metricsdb
             | STATS avg(metric.value.double) WHERE metric.name == "jvm.memory.total"
            """)) {
            double avgJvmMemoryTotal = (double) resp.column(0).next();
            assertThat(avgJvmMemoryTotal, greaterThan(0.0));
        }
    }

    protected EsqlQueryResponse query(String esql) {
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(client()).query(esql).execute().actionGet();
    }
}
