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
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDestination;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * End to end coverage of derived metrics: documents written to a source data stream should show up as compact metric documents in the
 * managed destination stream without the writer doing anything beyond configuring the option.
 */
public class DerivedMetricsIT extends ESIntegTestCase {

    private static final TimeValue INTERVAL = TimeValue.timeValueSeconds(1);

    /**
     * Long enough for an interval to close, the grace period to pass, and a flush to run.
     */
    private static final TimeValue FLUSH_WINDOW = TimeValue.timeValueSeconds(5);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // keep the test's wall clock short: flush every second and give in flight writes a minimal grace period
            .put("data_streams.derived_metrics.flush_interval", "1s")
            .put("data_streams.derived_metrics.flush_grace_period", "1s")
            .build();
    }

    public void testBuiltinIngestMetricsAreEmitted() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(null, List.of("ingest.*"), List.of(INTERVAL), null, null)
        );

        int documents = randomIntBetween(5, 20);
        for (int i = 0; i < documents; i++) {
            index(dataStream, Map.of("service.name", "checkout"));
        }

        assertBusy(() -> {
            Map<String, Double> metrics = summedValuesByMetric(dataStream);
            assertThat(metrics, hasKey("ingest.docs.count"));
            assertThat(metrics.get("ingest.docs.count"), equalTo((double) documents));
            // the interval is one second, so a per-second rate is numerically the same as the count
            assertThat(metrics.get("ingest.docs.rate"), equalTo((double) documents));
            assertThat(metrics.get("ingest.bytes.count"), greaterThan(0.0));
        });
    }

    public void testUserCounterAndGaugeMetrics() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(
                null,
                List.of(),
                List.of(INTERVAL),
                List.of("service.name"),
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "http.errors",
                        DataStreamDerivedMetrics.MetricType.COUNTER,
                        Map.of("range", Map.of("http.response.status_code", Map.of("gte", 500))),
                        null,
                        null,
                        null
                    ),
                    new DataStreamDerivedMetrics.Metric(
                        "queue.depth",
                        DataStreamDerivedMetrics.MetricType.GAUGE,
                        null,
                        DataStreamDerivedMetrics.MetricValue.field("queue.depth"),
                        DataStreamDerivedMetrics.GaugeAggregation.MAX,
                        null
                    )
                )
            )
        );

        index(dataStream, Map.of("service.name", "checkout", "http.response.status_code", 200, "queue.depth", 3));
        index(dataStream, Map.of("service.name", "checkout", "http.response.status_code", 503, "queue.depth", 9));
        index(dataStream, Map.of("service.name", "checkout", "http.response.status_code", 500, "queue.depth", 5));

        assertBusy(() -> {
            Map<String, Double> metrics = summedValuesByMetric(dataStream);
            // only the two 5xx documents match the predicate
            assertThat(metrics.get("http.errors"), equalTo(2.0));
            assertThat(metrics.get("queue.depth"), equalTo(9.0));
        });
    }

    public void testConfiguredDimensionsAreEmitted() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(null, List.of("ingest.docs.count"), List.of(INTERVAL), List.of("service.name"), null)
        );

        index(dataStream, Map.of("service.name", "checkout"));
        index(dataStream, Map.of("service.name", "checkout"));
        index(dataStream, Map.of("service.name", "search"));

        assertBusy(() -> {
            Map<String, Double> byService = new HashMap<>();
            for (Map<String, Object> document : metricDocuments(dataStream, "ingest.docs.count")) {
                assertThat(field(document, "derived_metrics.source"), equalTo(dataStream));
                assertThat(field(document, "derived_metrics.interval"), equalTo("1s"));
                byService.merge((String) field(document, "dimensions.service.name"), value(document), Double::sum);
            }
            assertThat(byService.get("checkout"), equalTo(2.0));
            assertThat(byService.get("search"), equalTo(1.0));
        });
    }

    public void testNoMetricsAreEmittedForAStreamWithoutDerivedMetrics() throws Exception {
        assertNothingIsEmitted(createDataStream(null));
    }

    public void testDisabledDerivedMetricsEmitNothing() throws Exception {
        assertNothingIsEmitted(
            createDataStream(new DataStreamDerivedMetrics.Template(false, List.of("ingest.*"), List.of(INTERVAL), null, null))
        );
    }

    private void assertNothingIsEmitted(String dataStream) {
        for (int i = 0; i < 5; i++) {
            index(dataStream, Map.of("service.name", "checkout"));
        }
        // give any buffered interval more than enough time to close and flush before concluding that nothing was emitted
        safeSleep(FLUSH_WINDOW);
        assertFalse(destinationExists(dataStream));
    }

    private String createDataStream(DataStreamDerivedMetrics.Template derivedMetrics) {
        String dataStream = "logs-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT) + "-default";
        Template.Builder template = Template.builder().settings(indexSettings(1, 0).build());
        if (derivedMetrics != null) {
            template.dataStreamOptions(new DataStreamOptions.Template(derivedMetrics));
        }
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

    private boolean destinationExists(String sourceDataStream) {
        // the destination is hidden, so it is looked up by name rather than through a wildcard expression
        String destination = DerivedMetricsDestination.destinationFor(sourceDataStream);
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .dataStreams()
            .containsKey(destination);
    }

    /**
     * Every partial series emitted for the given metric, summed the way a query that reduces across the emitting node dimension would.
     */
    private Map<String, Double> summedValuesByMetric(String sourceDataStream) {
        Map<String, Double> values = new HashMap<>();
        for (Map<String, Object> document : metricDocuments(sourceDataStream, null)) {
            values.merge((String) field(document, "metric.name"), value(document), Double::sum);
        }
        assertThat(values.keySet(), not(empty()));
        return values;
    }

    /**
     * The destination is a time series data stream, so its source comes back in object form rather than with the dotted keys the
     * emitter wrote. Paths are resolved rather than looked up directly so the assertions do not depend on that.
     */
    private static Object field(Map<String, Object> document, String path) {
        return XContentMapValues.extractValue(path, document);
    }

    private static double value(Map<String, Object> document) {
        return ((Number) field(document, "metric.value")).doubleValue();
    }

    private List<Map<String, Object>> metricDocuments(String sourceDataStream, String metricName) {
        assertTrue("no derived metrics were emitted for [" + sourceDataStream + "] yet", destinationExists(sourceDataStream));
        refresh(DerivedMetricsDestination.destinationFor(sourceDataStream));
        List<Map<String, Object>> documents = new ArrayList<>();
        var response = client().prepareSearch(DerivedMetricsDestination.destinationFor(sourceDataStream))
            .setSize(1000)
            .setQuery(metricName == null ? QueryBuilders.matchAllQuery() : QueryBuilders.termQuery("metric.name", metricName))
            .get();
        try {
            for (SearchHit hit : response.getHits()) {
                documents.add(hit.getSourceAsMap());
            }
        } finally {
            response.decRef();
        }
        assertThat(documents.size(), greaterThan(0));
        return documents;
    }
}
