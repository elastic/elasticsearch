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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDestination;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDestinationLifecycle;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsService;
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
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
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
        // the destination maps metric.histogram as exponential_histogram, whose mapper ships in x-pack-analytics
        return List.of(DataStreamsPlugin.class, DerivedMetricsHistogramMapperPlugin.class);
    }

    /**
     * The registry reinstalls the managed templates as soon as it sees them missing, so letting the test framework delete them between
     * tests just races it: the index template comes back and then the component template it composes cannot be removed.
     */
    @Override
    protected Set<String> excludeTemplates() {
        return Set.of(DerivedMetricsDestination.TEMPLATE_NAME, DerivedMetricsDestination.SETTINGS_COMPONENT_NAME);
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
        String dataStream = createDataStream(new DataStreamDerivedMetrics.Template(null, List.of("ingest.*"), INTERVAL, null, null, null));

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
                INTERVAL,
                null,
                List.of("service.name"),
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "http.errors",
                        DataStreamDerivedMetrics.MetricType.COUNTER,
                        Map.of("range", Map.of("http.response.status_code", Map.of("gte", 500))),
                        null,
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
                        null,
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

    /**
     * A histogram metric emits a distribution rather than a value. The assertions read the emitted documents back rather than
     * aggregating over them: the destination really is mapped as {@code exponential_histogram}, so the document round tripping through
     * synthetic source at all is the proof it was accepted as one, and the totals are what a consumer merging the partials would get.
     */
    public void testHistogramMetricsAreEmittedAsDistributions() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(
                null,
                List.of(),
                INTERVAL,
                null,
                null,
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "latency.distribution",
                        DataStreamDerivedMetrics.MetricType.HISTOGRAM,
                        null,
                        DataStreamDerivedMetrics.MetricValue.field("event.duration"),
                        null,
                        null,
                        null
                    )
                )
            )
        );

        for (int duration = 1; duration <= 100; duration++) {
            index(dataStream, Map.of("service.name", "checkout", "event.duration", duration));
        }

        assertBusy(() -> {
            double sum = 0.0;
            long observations = 0L;
            double slowest = Double.NEGATIVE_INFINITY;
            for (Map<String, Object> document : metricDocuments(dataStream, "latency.distribution")) {
                assertThat(document, not(hasKey("metric.value")));
                sum += ((Number) field(document, "metric.histogram.sum")).doubleValue();
                slowest = Math.max(slowest, ((Number) field(document, "metric.histogram.max")).doubleValue());
                for (Object count : (List<?>) field(document, "metric.histogram.positive.counts")) {
                    observations += ((Number) count).longValue();
                }
            }
            // every observation is in the distribution, however many partials it ended up split across
            assertThat(observations, equalTo(100L));
            // the histogram carries the sum exactly, which is why no separate metric.value travels with it
            assertThat(sum, closeTo(5050.0, 1e-6));
            // bucket boundaries are approximate, so an individual value only comes back to within the bucket that holds it
            assertThat(slowest, closeTo(100.0, 1.0));
        });
    }

    public void testConfiguredDimensionsAreEmitted() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(null, List.of("ingest.docs.count"), INTERVAL, null, List.of("service.name"), null)
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

    /**
     * A metric that overrides the interval is written to that interval's own destination, leaving the default destination untouched.
     */
    public void testIntervalOverrideWritesToItsOwnDestination() throws Exception {
        TimeValue override = TimeValue.timeValueSeconds(2);
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(
                null,
                List.of("ingest.docs.count"),
                INTERVAL,
                List.of(new DataStreamDerivedMetrics.Destination(override, null)),
                null,
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "queue.depth",
                        DataStreamDerivedMetrics.MetricType.GAUGE,
                        null,
                        DataStreamDerivedMetrics.MetricValue.field("queue.depth"),
                        DataStreamDerivedMetrics.GaugeAggregation.MAX,
                        null,
                        override
                    )
                )
            )
        );

        index(dataStream, Map.of("service.name", "checkout", "queue.depth", 7));

        assertBusy(() -> {
            assertThat(metricNamesIn(destination(dataStream, INTERVAL)), contains("ingest.docs.count"));
            assertThat(metricNamesIn(destination(dataStream, override)), contains("queue.depth"));
        });
    }

    /**
     * A destination is created by the first document written to it, from a template that carries no lifecycle. The retention configured
     * on the source has to be applied to the destination afterwards.
     */
    public void testDestinationIsGivenTheConfiguredRetention() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(
                null,
                List.of("ingest.docs.count"),
                INTERVAL,
                List.of(
                    new DataStreamDerivedMetrics.Destination(
                        INTERVAL,
                        DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(7)).buildTemplate()
                    )
                ),
                null,
                null
            )
        );

        index(dataStream, Map.of("service.name", "checkout"));

        assertBusy(() -> {
            DataStreamLifecycle lifecycle = lifecycleOf(destination(dataStream, INTERVAL));
            assertNotNull("the destination has no lifecycle yet", lifecycle);
            assertThat(lifecycle.dataRetention(), equalTo(TimeValue.timeValueDays(7)));
        });
    }

    /**
     * Without a declared destination the retention falls back to a default, so a destination is never unbounded by accident.
     */
    public void testDestinationFallsBackToTheDefaultRetention() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(null, List.of("ingest.docs.count"), INTERVAL, null, null, null)
        );

        index(dataStream, Map.of("service.name", "checkout"));

        assertBusy(() -> {
            DataStreamLifecycle lifecycle = lifecycleOf(destination(dataStream, INTERVAL));
            assertNotNull("the destination has no lifecycle yet", lifecycle);
            assertThat(lifecycle.dataRetention(), equalTo(DerivedMetricsDestinationLifecycle.FALLBACK_RETENTION));
        });
    }

    /**
     * The buffer allocates through BigArrays against its own breaker, so its memory is bounded and an operator can see it rather than
     * having to infer it. This asserts the plugin wiring; that the accounting actually moves is covered by the buffer's own tests.
     */
    public void testTheDerivedMetricsBreakerIsRegistered() {
        var nodes = clusterAdmin().prepareNodesStats().setBreaker(true).get().getNodes();
        assertFalse(nodes.isEmpty());
        for (var node : nodes) {
            var stats = node.getBreaker().getStats(DerivedMetricsService.BREAKER_NAME);
            assertNotNull("the derived metrics breaker should appear in node stats", stats);
            assertThat("its limit should be derived from the heap", stats.getLimit(), greaterThan(0L));
        }
    }

    private DataStreamLifecycle lifecycleOf(String dataStream) {
        DataStream found = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .dataStreams()
            .get(dataStream);
        assertNotNull("[" + dataStream + "] does not exist yet", found);
        return found.getDataLifecycle();
    }

    private String destination(String dataStream, TimeValue interval) {
        return DerivedMetricsDestination.destinationFor(dataStream, interval.getStringRep());
    }

    private List<String> metricNamesIn(String destination) {
        assertTrue("[" + destination + "] does not exist yet", dataStreamExists(destination));
        refresh(destination);
        List<String> names = new ArrayList<>();
        var response = client().prepareSearch(destination).setSize(100).setQuery(QueryBuilders.matchAllQuery()).get();
        try {
            for (SearchHit hit : response.getHits()) {
                String name = (String) field(hit.getSourceAsMap(), "metric.name");
                if (names.contains(name) == false) {
                    names.add(name);
                }
            }
        } finally {
            response.decRef();
        }
        return names;
    }

    public void testNoMetricsAreEmittedForAStreamWithoutDerivedMetrics() throws Exception {
        assertNothingIsEmitted(createDataStream(null));
    }

    public void testDisabledDerivedMetricsEmitNothing() throws Exception {
        assertNothingIsEmitted(
            createDataStream(new DataStreamDerivedMetrics.Template(false, List.of("ingest.*"), INTERVAL, null, null, null))
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
        return dataStreamExists(destination(sourceDataStream, INTERVAL));
    }

    // the destinations are hidden, so they are looked up by name rather than through a wildcard expression
    private boolean dataStreamExists(String destination) {
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
        refresh(DerivedMetricsDestination.destinationFor(sourceDataStream, INTERVAL.getStringRep()));
        List<Map<String, Object>> documents = new ArrayList<>();
        var response = client().prepareSearch(DerivedMetricsDestination.destinationFor(sourceDataStream, INTERVAL.getStringRep()))
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
