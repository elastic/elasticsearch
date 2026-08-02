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
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.in;

/**
 * The one thing the rest of the suite never exercises: more than one node observing the same data stream.
 *
 * <p>Every other integration test pins the source to a single shard, so every primary write lands on one node and
 * {@code derived_metrics.node} only ever takes one value. That leaves the central design commitment — each node emits its own partial and
 * queries reduce across the node dimension — completely untested, including the part every consumer has to get right.
 *
 * <p>These tests use several shards across several nodes so partials genuinely come from different places, and assert the property that
 * makes the whole scheme sound: recombining the partials reproduces the answer a scan of the source would give.
 */
@ClusterScope(numDataNodes = 3)
public class DerivedMetricsMultiNodeIT extends ESIntegTestCase {

    private static final TimeValue INTERVAL = TimeValue.timeValueSeconds(1);

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
     * The headline property. Documents spread across shards on different nodes, each node counts what it saw, and the sum over the node
     * dimension is the true total. If partials were double counted or lost this is where it shows.
     */
    public void testPartialsFromSeveralNodesSumToTheTruth() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(null, List.of("ingest.docs.count"), INTERVAL, null, null, null)
        );

        int documents = 300;
        for (int i = 0; i < documents; i++) {
            index(dataStream, Map.of("service.name", "checkout", "queue.depth", i));
        }

        assertBusy(() -> {
            List<Map<String, Object>> emitted = metricDocuments(dataStream, "ingest.docs.count");
            Set<String> nodes = new HashSet<>();
            Set<String> names = new HashSet<>();
            double total = 0.0;
            for (Map<String, Object> document : emitted) {
                nodes.add((String) field(document, "derived_metrics.node"));
                names.add((String) field(document, "derived_metrics.node_name"));
                total += ((Number) field(document, "metric.value")).doubleValue();
            }
            // the dimension is the persistent ID, and the name rides along so a dashboard is legible; the two must agree on how many
            // nodes there were, or one of them is not identifying what it claims to
            assertThat(names, everyItem(in(Set.of(internalCluster().getNodeNames()))));
            assertThat(names.size(), equalTo(nodes.size()));
            // the point of the test: the work really was spread, so the sum really is a cross-node sum
            assertThat("expected partials from more than one node", nodes.size(), greaterThan(1));
            assertThat(total, equalTo((double) documents));
        });
    }

    /**
     * A max gauge combines by taking the maximum across nodes, and every document says so itself now, so a consumer does not have to go
     * and read the source stream's configuration to find out.
     */
    public void testMaxGaugeCombinesAcrossNodes() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(
                null,
                List.of(),
                INTERVAL,
                null,
                null,
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "queue.depth.max",
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

        int documents = 300;
        for (int i = 0; i < documents; i++) {
            index(dataStream, Map.of("service.name", "checkout", "queue.depth", i));
        }

        assertBusy(() -> {
            double highest = Double.NEGATIVE_INFINITY;
            for (Map<String, Object> document : metricDocuments(dataStream, "queue.depth.max")) {
                assertThat(field(document, "derived_metrics.reduction"), equalTo("max"));
                highest = Math.max(highest, ((Number) field(document, "metric.value")).doubleValue());
            }
            assertThat(highest, equalTo((double) documents - 1));
        });
    }

    /**
     * The write path avoids re-parsing {@code _source} when the mapping lets every configured value be recovered from the document
     * Elasticsearch has already parsed. That is the great majority of what observing a write costs, so it is worth proving the fast path
     * is actually taken in a running cluster rather than silently falling back and leaving the optimisation dead.
     */
    public void testValuesAreReadFromTheParsedDocumentRatherThanReparsed() throws Exception {
        String dataStream = createDataStream(
            // a dimension forces the write path to read values at all; without one it never touches the document
            new DataStreamDerivedMetrics.Template(null, List.of("ingest.docs.count"), INTERVAL, null, List.of("service.name"), null)
        );

        for (int i = 0; i < 100; i++) {
            index(dataStream, Map.of("service.name", "checkout", "queue.depth", i));
        }
        // let the interval close and emit, so the counters are settled and nothing is still in flight at teardown
        assertBusy(() -> assertThat(metricDocuments(dataStream, "ingest.docs.count").size(), greaterThan(0)));

        long fromIndex = 0;
        long fromSource = 0;
        for (DerivedMetricsService service : internalCluster().getInstances(DerivedMetricsService.class)) {
            fromIndex += service.documentsReadFromIndex();
            fromSource += service.documentsReadFromSource();
        }
        assertThat("every document should have been read without a second parse", fromIndex, greaterThan(0L));
        assertThat("and none of them should have needed one", fromSource, equalTo(0L));
    }

    private String createDataStream(DataStreamDerivedMetrics.Template derivedMetrics) {
        String dataStream = "logs-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT) + "-default";
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            "derived-metrics-multinode-template"
        );
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStream))
                .template(
                    // several shards so the writes, and therefore the observations, land on more than one node
                    Template.builder()
                        .settings(indexSettings(3, 0).build())
                        .dataStreamOptions(new DataStreamOptions.Template(derivedMetrics))
                )
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
        ensureGreen(dataStream);
        return dataStream;
    }

    private void index(String dataStream, Map<String, Object> fields) {
        Map<String, Object> source = new HashMap<>(fields);
        source.put("@timestamp", System.currentTimeMillis());
        client().index(new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE).source(source)).actionGet();
    }

    private static Object field(Map<String, Object> document, String path) {
        return XContentMapValues.extractValue(path, document);
    }

    private List<Map<String, Object>> metricDocuments(String sourceDataStream, String metricName) {
        String destination = DerivedMetricsDestination.destinationFor(sourceDataStream, INTERVAL.getStringRep());
        assertTrue(
            "no derived metrics were emitted for [" + sourceDataStream + "] yet",
            clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .metadata()
                .getProject()
                .dataStreams()
                .containsKey(destination)
        );
        refresh(destination);
        List<Map<String, Object>> documents = new ArrayList<>();
        var response = client().prepareSearch(destination).setSize(1000).setQuery(QueryBuilders.termQuery("metric.name", metricName)).get();
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
