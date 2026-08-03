/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDestination;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The stats API exists to answer "which metric is spending my series budget, on which stream, and through which dimension". Proving it
 * returns 200 proves nothing, so these tests write documents with a known number of distinct dimension values and check the numbers that
 * come back are the numbers that went in.
 */
public class DerivedMetricsStatsIT extends DerivedMetricsIntegTestCase {

    private static final TimeValue INTERVAL = TimeValue.timeValueSeconds(10);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, DerivedMetricsHistogramMapperPlugin.class);
    }

    @Override
    protected Set<String> excludeTemplates() {
        return Set.of(DerivedMetricsDestination.TEMPLATE_NAME, DerivedMetricsDestination.SETTINGS_COMPONENT_NAME);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("data_streams.derived_metrics.flush_interval", "1s")
            .put("data_streams.derived_metrics.flush_grace_period", "1s")
            .build();
    }

    /**
     * The whole point of the API: a dimension's cardinality is the thing nothing else could report, and it has to survive the reduction
     * across nodes and reach a reader with a sane value.
     */
    public void testAMetricAndItsDimensionCardinalityReachTheApi() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(
                null,
                List.of(),
                INTERVAL,
                null,
                List.of(),
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "http.requests",
                        DataStreamDerivedMetrics.MetricType.COUNTER,
                        null,
                        null,
                        null,
                        List.of("service.name"),
                        null,
                        null
                    )
                )
            )
        );

        int services = 7;
        for (int document = 0; document < services * 4; document++) {
            index(dataStream, Map.of("service.name", "service-" + (document % services)));
        }

        assertBusy(() -> {
            GetDerivedMetricsStatsAction.DataStreamStats stats = statsFor(dataStream);
            assertNotNull("the stats API never reported [" + dataStream + "]", stats);

            GetDerivedMetricsStatsAction.MetricStats metric = stats.metrics()
                .stream()
                .filter(candidate -> candidate.name().equals("http.requests"))
                .findFirst()
                .orElse(null);
            assertNotNull("the stats API never reported the configured metric", metric);
            assertThat(metric.interval(), equalTo("10s"));
            assertFalse(metric.histogram());

            assertThat(metric.dimensions().size(), equalTo(1));
            GetDerivedMetricsStatsAction.DimensionStats dimension = metric.dimensions().get(0);
            assertThat(dimension.name(), equalTo("service.name"));
            // The estimate is a HyperLogLog sketch, so it is approximate — but at this cardinality linear counting is exact, and it can
            // never legitimately exceed the values actually written.
            assertThat(dimension.estimatedDistinctValues(), equalTo((long) services));
            assertFalse("nothing here is anywhere near the dimension budget", dimension.collapsed());
        });
    }

    /**
     * A series is buffered between the write and the flush that emits it, and while it is the API has to say so — including what it is
     * charged to the circuit breaker for, which is the number capacity planning actually needs.
     */
    public void testSeriesAndBytesHeldAreReportedWhileTheyAreBuffered() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(null, List.of("ingest.*"), INTERVAL, null, List.of("service.name"), List.of())
        );

        int services = 3;
        for (int document = 0; document < services * 3; document++) {
            index(dataStream, Map.of("service.name", "service-" + (document % services)));
        }

        assertBusy(() -> {
            GetDerivedMetricsStatsAction.DataStreamStats stats = statsFor(dataStream);
            assertNotNull(stats);
            assertThat(stats.seriesHeld(), greaterThanOrEqualTo((long) services));
            assertThat(stats.bytesHeld(), greaterThan(0L));
            assertThat(stats.histogramSeriesHeld(), equalTo(0L));
            // nothing here is over budget, so nothing should have been refused
            assertThat(stats.refusals().total(), equalTo(0L));

            GetDerivedMetricsStatsAction.MetricStats counts = stats.metrics()
                .stream()
                .filter(metric -> metric.name().equals("ingest.docs.count"))
                .findFirst()
                .orElse(null);
            assertNotNull("the built-in ingest metrics were never reported", counts);
            assertThat(counts.seriesHeld(), greaterThanOrEqualTo((long) services));
            assertThat(counts.bytesHeld(), greaterThan(0L));
            assertThat(counts.seriesHeld(), lessThanOrEqualTo(stats.seriesHeld()));
        });
    }

    /**
     * The API answers on a cluster where the feature is not in use, every node takes part, and a stream nobody configured is absent rather
     * than reported as zero — a report listing every data stream in the cluster would bury the handful that cost anything.
     */
    public void testAStreamThatWasNeverConfiguredIsNotReported() {
        GetDerivedMetricsStatsAction.Response response = stats();
        assertThat(response.getNodes().size(), equalTo(internalCluster().size()));
        assertThat(response.failures(), equalTo(List.of()));
        assertNull(statsFor("logs-never-configured-default"));
    }

    private GetDerivedMetricsStatsAction.Response stats() {
        return client().execute(GetDerivedMetricsStatsAction.INSTANCE, new GetDerivedMetricsStatsAction.Request()).actionGet();
    }

    private GetDerivedMetricsStatsAction.DataStreamStats statsFor(String dataStream) {
        return stats().dataStreams().stream().filter(stats -> stats.name().equals(dataStream)).findFirst().orElse(null);
    }

    private String createDataStream(DataStreamDerivedMetrics.Template derivedMetrics) {
        String dataStream = "logs-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT) + "-default";
        Template.Builder template = Template.builder().settings(indexSettings(1, 0).build());
        template.dataStreamOptions(new DataStreamOptions.Template(derivedMetrics));
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            "derived-metrics-source-template"
        );
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStream))
                .template(template)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet());
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStream)
            ).actionGet()
        );
        return dataStream;
    }

    private void index(String dataStream, Map<String, Object> fields) {
        Map<String, Object> source = new HashMap<>(fields);
        source.put("@timestamp", System.currentTimeMillis());
        client().index(new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE).source(source)).actionGet();
    }
}
