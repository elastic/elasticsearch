/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class MetricsDBIndexingIT extends ESSingleNodeTestCase {

    private SdkMeterProvider meterProvider;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class, InternalSettingsPlugin.class, MetricsPlugin.class);
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
        var mappingTemplate = """
            {
              "_doc":{
                "properties": {
                  "@timestamp" : {
                    "type": "date"
                  },
                  "dummy": {
                    "type": "keyword",
                    "time_series_dimension": true
                  }
                }
              }
            }""";

        var request = new TransportPutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("metricsdb"))
                .template(
                    new Template(Settings.builder().put("index.mode", "time_series").build(), new CompressedXContent(mappingTemplate), null)
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .priority(500L)
                .build()
        );
        var res = client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
        assertThat(res.isAcknowledged(), is(true));

        OtlpHttpMetricExporter exporter = OtlpHttpMetricExporter.builder()
            .setEndpoint("http://localhost:" + getHttpPort() + "/_otlp/v1/metrics")
            .build();

        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.builder(exporter).build())
            .build();
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
    public void testIngestMetric() {
        Meter sampleMeter = meterProvider.get("io.opentelemetry.example.metrics");

        sampleMeter.gaugeBuilder("jvm.memory.total")
            .setDescription("Reports JVM memory usage.")
            .setUnit("By")
            .buildWithCallback(result -> result.record(Runtime.getRuntime().totalMemory(), Attributes.empty()));

        var result = meterProvider.forceFlush().join(1, TimeUnit.SECONDS);
        assertThat(result.isSuccess(), is(true));

        admin().indices().prepareRefresh().execute().actionGet();
        long hits = client().prepareSearch("metricsdb")
            .setQuery(QueryBuilders.matchAllQuery())
            .execute()
            .actionGet()
            .getHits()
            .getTotalHits()
            .value();
        assertThat(hits, greaterThan(0L));
    }
}
