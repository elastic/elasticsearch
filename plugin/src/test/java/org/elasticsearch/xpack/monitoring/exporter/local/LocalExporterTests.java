/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.local;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkDoc;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkRequestBuilder;
import org.elasticsearch.xpack.monitoring.action.MonitoringIndex;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.monitoring.MonitoredSystem.BEATS;
import static org.elasticsearch.xpack.monitoring.MonitoredSystem.KIBANA;
import static org.elasticsearch.xpack.monitoring.MonitoredSystem.LOGSTASH;
import static org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils.DATA_INDEX;
import static org.hamcrest.Matchers.greaterThan;

public class LocalExporterTests extends MonitoringIntegTestCase {

    private static SetOnce<String> indexTimeFormat = new SetOnce<>();

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        String customTimeFormat = null;
        if (randomBoolean()) {
            customTimeFormat = randomFrom("YY", "YYYY", "YYYY.MM", "YYYY-MM", "MM.YYYY", "MM");
        }
        indexTimeFormat.set(customTimeFormat);
        return super.buildTestCluster(scope, seed);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.exporters._local.enabled", false)
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .build();
    }

    @AfterClass
    public static void cleanUp() {
        indexTimeFormat = null;
    }

    @After
    public void stopMonitoring() throws Exception {
        Settings.Builder exporterSettings = Settings.builder()
                .putNull("xpack.monitoring.exporters._local.enabled")
                .putNull("xpack.monitoring.exporters._local.index.name.time_format")
                .putNull(MonitoringSettings.INTERVAL.getKey());

        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(exporterSettings));

        wipeMonitoringIndices();
    }

    public void testExport() throws Exception {
        if (randomBoolean()) {
            // indexing some random documents
            IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
            for (int i = 0; i < indexRequestBuilders.length; i++) {
                indexRequestBuilders[i] = client().prepareIndex("test", "type", Integer.toString(i))
                        .setSource("title", "This is a random document");
            }
            indexRandom(true, indexRequestBuilders);
        }

        if (randomBoolean()) {
            // create some marvel indices to check if aliases are correctly created
            final int oldies = randomIntBetween(1, 20);
            for (int i = 0; i < oldies; i++) {
                assertAcked(client().admin().indices().prepareCreate(".marvel-es-1-2014.12." + i)
                        .setSettings("number_of_shards", 1, "number_of_replicas", 0).get());
            }
        }

        if (randomBoolean()) {
            // create the monitoring data index to check if its mappings are correctly updated
            createIndex(DATA_INDEX);
        }

        Settings.Builder exporterSettings = Settings.builder()
                .put("xpack.monitoring.exporters._local.enabled", true);

        String timeFormat = indexTimeFormat.get();
        if (timeFormat != null) {
            exporterSettings.put("xpack.monitoring.exporters._local.index.name.time_format",
                    timeFormat);
        }

        // local exporter is now enabled
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(exporterSettings));

        if (randomBoolean()) {
            // export some documents now, before starting the monitoring service
            final int nbDocs = randomIntBetween(1, 20);
            List<MonitoringBulkDoc> monitoringDocs = new ArrayList<>(nbDocs);
            for (int i = 0; i < nbDocs; i++) {
                monitoringDocs.add(createMonitoringBulkDoc(String.valueOf(i)));
            }

            assertBusy(() -> {
                MonitoringBulkRequestBuilder bulk = monitoringClient().prepareMonitoringBulk();
                monitoringDocs.forEach(bulk::add);
                assertEquals(RestStatus.OK, bulk.get().status());
                refresh();

                SearchResponse response = client().prepareSearch(".monitoring-*").get();
                assertEquals(nbDocs, response.getHits().getTotalHits());
            });

            checkMonitoringTemplates();
            checkMonitoringPipeline();
            checkMonitoringAliases();
            checkMonitoringMappings();
            checkMonitoringDocs();
        }

        // monitoring service is started
        exporterSettings = Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), 3L, TimeUnit.SECONDS);
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(exporterSettings));

        final int numNodes = internalCluster().getNodeNames().length;
        assertBusy(() -> {
            refresh(".monitoring-*");
            assertThat(client().prepareSearch(".monitoring-es-2-*").setTypes("cluster_state")
                    .get().getHits().getTotalHits(), greaterThan(0L));

            assertEquals(0L, client().prepareSearch(".monitoring-es-2-*").setTypes("node")
                    .get().getHits().getTotalHits() % numNodes);

            assertThat(client().prepareSearch(".monitoring-es-2-*").setTypes("cluster_stats")
                    .get().getHits().getTotalHits(), greaterThan(0L));

            assertThat(client().prepareSearch(".monitoring-es-2-*").setTypes("index_recovery")
                    .get().getHits().getTotalHits(), greaterThan(0L));

            assertThat(client().prepareSearch(".monitoring-es-2-*").setTypes("index_stats")
                    .get().getHits().getTotalHits(), greaterThan(0L));

            assertThat(client().prepareSearch(".monitoring-es-2-*").setTypes("indices_stats")
                    .get().getHits().getTotalHits(), greaterThan(0L));

            assertThat(client().prepareSearch(".monitoring-es-2-*").setTypes("shards")
                    .get().getHits().getTotalHits(), greaterThan(0L));

            assertThat(client().prepareSearch(".monitoring-data-2").setTypes("cluster_info")
                    .get().getHits().getTotalHits(), greaterThan(0L));

            assertEquals(numNodes, client().prepareSearch(".monitoring-data-2").setTypes("node")
                    .get().getHits().getTotalHits());

            SearchResponse response = client().prepareSearch(".monitoring-es-2-*")
                    .setTypes("node_stats")
                    .setSize(0)
                    .addAggregation(terms("agg_nodes_ids").field("node_stats.node_id"))
                    .get();

            StringTerms aggregation = response.getAggregations().get("agg_nodes_ids");
            assertEquals("Aggregation on node_id must return a bucket per node involved in test",
                    numNodes, aggregation.getBuckets().size());

            for (String nodeName : internalCluster().getNodeNames()) {
                String nodeId = internalCluster().clusterService(nodeName).localNode().getId();
                assertTrue(aggregation.getBucketByKey(nodeId).getDocCount() >= 1L);
            }

        }, 30L, TimeUnit.SECONDS);

        checkMonitoringTemplates();
        checkMonitoringPipeline();
        checkMonitoringAliases();
        checkMonitoringMappings();
        checkMonitoringDocs();
    }

    /**
     * Checks that the monitoring templates have been created by the local exporter
     */
    private void checkMonitoringTemplates() {
        final Set<String> templates = new HashSet<>();
        templates.add(".monitoring-data-2");
        templates.add(".monitoring-alerts-2");
        for (MonitoredSystem system : MonitoredSystem.values()) {
            templates.add(String.join("-", ".monitoring", system.getSystem(), "2"));
        }

        GetIndexTemplatesResponse response =
                client().admin().indices().prepareGetTemplates(".monitoring-*").get();
        Set<String> actualTemplates = response.getIndexTemplates().stream()
                .map(IndexTemplateMetaData::getName).collect(Collectors.toSet());
        assertEquals(templates, actualTemplates);
    }

    /**
     * Checks that the monitoring ingest pipeline have been created by the local exporter
     */
    private void checkMonitoringPipeline() {
        GetPipelineResponse response =
                client().admin().cluster().prepareGetPipeline(Exporter.EXPORT_PIPELINE_NAME).get();
        assertTrue("monitoring ingest pipeline not found", response.isFound());
    }

    /**
     * Checks that the local exporter correctly added aliases to indices created with previous
     * Marvel versions.
     */
    private void checkMonitoringAliases() {
        GetIndexResponse response =
                client().admin().indices().prepareGetIndex().setIndices(".marvel-es-1-*").get();
        for (String index : response.getIndices()) {
            List<AliasMetaData> aliases = response.getAliases().get(index);
            assertEquals("marvel index should have at least 1 alias: " + index, 1, aliases.size());

            String indexDate = index.substring(".marvel-es-1-".length());
            String expectedAlias = ".monitoring-es-2-" + indexDate + "-alias";
            assertEquals(expectedAlias, aliases.get(0).getAlias());
        }
    }

    /**
     * Checks that the local exporter correctly updated the mappings of an existing data index.
     */
    private void checkMonitoringMappings() {
        IndicesExistsResponse exists = client().admin().indices().prepareExists(DATA_INDEX).get();
        if (exists.isExists()) {
            GetMappingsResponse response =
                    client().admin().indices().prepareGetMappings(DATA_INDEX).get();
            for (String mapping : MonitoringTemplateUtils.NEW_DATA_TYPES) {
                assertTrue("mapping [" + mapping + "] should exist in data index",
                        response.getMappings().get(DATA_INDEX).containsKey(mapping));
            }
        }
    }

    /**
     * Checks that the monitoring documents all have the cluster_uuid, timestamp and source_node
     * fields and belongs to the right data or timestamped index.
     */
    private void checkMonitoringDocs() {
        ClusterStateResponse response = client().admin().cluster().prepareState().get();
        String customTimeFormat = response.getState().getMetaData().transientSettings()
                .get("xpack.monitoring.exporters._local.index.name.time_format");
        assertEquals(indexTimeFormat.get(), customTimeFormat);
        if (customTimeFormat == null) {
            customTimeFormat = "YYYY.MM.dd";
        }

        DateTimeFormatter dateParser = ISODateTimeFormat.dateTime().withZoneUTC();
        DateTimeFormatter dateFormatter = DateTimeFormat.forPattern(customTimeFormat).withZoneUTC();

        SearchResponse searchResponse = client().prepareSearch(".monitoring-*").setSize(100).get();
        assertThat(searchResponse.getHits().getTotalHits(), greaterThan(0L));

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, Object> source = hit.getSourceAsMap();
            assertTrue(source != null && source.isEmpty() == false);

            String clusterUUID = (String) source.get("cluster_uuid");
            assertTrue("document is missing cluster_uuid field", Strings.hasText(clusterUUID));

            String timestamp = (String) source.get("timestamp");
            assertTrue("document is missing timestamp field", Strings.hasText(timestamp));

            String type = hit.getType();
            assertTrue(Strings.hasText(type));

            Set<String> expectedIndex = new HashSet<>();
            if ("cluster_info".equals(type) || type.startsWith("data")) {
                expectedIndex.add(".monitoring-data-2");
            } else {
                MonitoredSystem system = MonitoredSystem.ES;
                if (type.startsWith("timestamped")) {
                    system = MonitoredSystem.fromSystem(type.substring(type.indexOf("_") + 1));
                }

                String dateTime = dateFormatter.print(dateParser.parseDateTime(timestamp));
                expectedIndex.add(".monitoring-" + system.getSystem() + "-2-" + dateTime);

                if ("node".equals(type)) {
                    expectedIndex.add(".monitoring-data-2");
                }
            }
            assertTrue(expectedIndex.contains(hit.getIndex()));

            @SuppressWarnings("unchecked")
            Map<String, Object> sourceNode = (Map<String, Object>) source.get("source_node");
            if ("shards".equals(type) == false) {
                assertNotNull("document is missing source_node field", sourceNode);
            }
        }
    }

    private static MonitoringBulkDoc createMonitoringBulkDoc(String id) throws IOException {
        String monitoringId = randomFrom(BEATS, KIBANA, LOGSTASH).getSystem();
        String monitoringVersion = MonitoringTemplateUtils.TEMPLATE_VERSION;
        MonitoringIndex index = randomFrom(MonitoringIndex.values());
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference source = RandomObjects.randomSource(random(), xContentType);

        // Aligns the type with the monitoring index and monitored system so that we can later
        // check if the document is indexed in the correct index.
        String type = index.name().toLowerCase(Locale.ROOT) + "_" + monitoringId;

        return new MonitoringBulkDoc(monitoringId, monitoringVersion, index, type, id, source,
                xContentType);
    }
}
