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

import static org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction.Request;
import static org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction.TYPE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Covers the {@code flush_early} response to memory pressure, which needs a node budget too small for the workload and so gets its own
 * suite rather than sharing {@link DerivedMetricsIT}'s node settings.
 *
 * <p>The property under test is that flushing early loses nothing. It is worth an integration test rather than a unit test because the
 * thing that could break it only exists in a real cluster: a time series {@code _id} is derived from the tsid and the timestamp, so two
 * partials of the same series in the same bucket would produce the same {@code _id} and the second would be rejected by
 * {@code op_type=create}. That rejection is invisible from the buffer's side and would simply show up as a total that reads low.
 */
public class DerivedMetricsFlushEarlyIT extends ESIntegTestCase {

    private static final TimeValue INTERVAL = TimeValue.timeValueSeconds(1);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("data_streams.derived_metrics.flush_interval", "1s")
            .put("data_streams.derived_metrics.flush_grace_period", "1s")
            // one series at a time, so every write for a second service forces the bucket out early
            .put("data_streams.derived_metrics.max_series_per_node", 1)
            .put("data_streams.derived_metrics.memory_pressure_policy", "flush_early")
            .build();
    }

    public void testPartialsOfOneBucketAllLandAndSumToTheWholeBucket() throws Exception {
        String dataStream = createDataStream(
            new DataStreamDerivedMetrics.Template(null, List.of("ingest.docs.count"), INTERVAL, null, List.of("service.name"), null)
        );

        // Alternating services with room for only one of them means the same service is drained, recorded again and drained again within
        // one bucket, which is exactly the case a deterministic _id would collide on.
        int documents = 20;
        for (int i = 0; i < documents; i++) {
            index(dataStream, Map.of("service.name", i % 2 == 0 ? "checkout" : "search"));
        }

        assertBusy(() -> {
            List<Map<String, Object>> emitted = metricDocuments(dataStream);
            double total = 0.0;
            for (Map<String, Object> document : emitted) {
                total += ((Number) XContentMapValues.extractValue("metric.value", document)).doubleValue();
            }
            // nothing was lost: every observation is accounted for once the partials are summed
            assertThat(total, equalTo((double) documents));
            // and it really was split into partials rather than fitting in one document per series
            assertThat(emitted.size(), greaterThan(2));
        });
    }

    private String createDataStream(DataStreamDerivedMetrics.Template derivedMetrics) {
        String dataStream = "logs-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT) + "-default";
        Request request = new Request("derived-metrics-source-template");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStream))
                .template(
                    Template.builder()
                        .settings(indexSettings(1, 0).build())
                        .dataStreamOptions(new DataStreamOptions.Template(derivedMetrics))
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TYPE, request).actionGet());
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

    private List<Map<String, Object>> metricDocuments(String sourceDataStream) {
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
        var response = client().prepareSearch(destination)
            .setSize(1000)
            .setQuery(QueryBuilders.termQuery("metric.name", "ingest.docs.count"))
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
