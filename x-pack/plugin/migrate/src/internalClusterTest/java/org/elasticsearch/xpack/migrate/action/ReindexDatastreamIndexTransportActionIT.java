/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.DeletePipelineTransportAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.migrate.MigratePlugin;
import org.elasticsearch.xpack.migrate.MigrateTemplateRegistry;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.lang.Boolean.parseBoolean;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDatastreamIndexTransportActionIT extends ESIntegTestCase {
    @After
    private void cleanupCluster() throws Exception {
        safeGet(
            clusterAdmin().execute(
                DeletePipelineTransportAction.TYPE,
                new DeletePipelineRequest(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    MigrateTemplateRegistry.REINDEX_DATA_STREAM_PIPELINE_NAME
                )
            )
        );
        super.cleanUpCluster();
    }

    private static final String MAPPING = """
        {
          "_doc":{
            "dynamic":"strict",
            "properties":{
              "foo1": {"type":"text"},
              "@timestamp": {"type":"date"}
            }
          }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            MigratePlugin.class,
            ReindexPlugin.class,
            MockTransportService.TestPlugin.class,
            DataStreamsPlugin.class,
            IngestCommonPlugin.class
        );
    }

    private static String DATA_STREAM_MAPPING = """
            {
                "dynamic": true,
                "_data_stream_timestamp": {
                    "enabled": true
                },
                "properties": {
                    "@timestamp": {"type":"date"}
                }
            }
        """;

    public void testTimestamp0AddedIfMissing() {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));

        // add doc without timestamp
        addDoc(sourceIndex, "{\"foo\":\"baz\"}");

        // add timestamp to source mapping
        indicesAdmin().preparePutMapping(sourceIndex).setSource(DATA_STREAM_MAPPING, XContentType.JSON).get();

        // call reindex
        var destIndex = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        ).getDestIndex();

        assertResponse(prepareSearch(destIndex), response -> {
            Map<String, Object> sourceAsMap = response.getHits().getAt(0).getSourceAsMap();
            assertEquals(Integer.valueOf(0), sourceAsMap.get(DEFAULT_TIMESTAMP_FIELD));
        });
    }

    public void testTimestampNotAddedIfExists() {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));

        // add doc with timestamp
        String time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        var doc = String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, time);
        addDoc(sourceIndex, doc);

        // add timestamp to source mapping
        indicesAdmin().preparePutMapping(sourceIndex).setSource(DATA_STREAM_MAPPING, XContentType.JSON).get();

        // call reindex
        var destIndex = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        ).getDestIndex();

        assertResponse(prepareSearch(destIndex), response -> {
            Map<String, Object> sourceAsMap = response.getHits().getAt(0).getSourceAsMap();
            assertEquals(time, sourceAsMap.get(DEFAULT_TIMESTAMP_FIELD));
        });
    }

    public void testCustomReindexPipeline() {
        String customPipeline = """
                {
                  "processors": [
                    {
                      "set": {
                        "field": "cheese",
                        "value": "gorgonzola"
                      }
                    }
                  ],
                  "version": 1000
                }
            """;

        PutPipelineRequest putRequest = new PutPipelineRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            MigrateTemplateRegistry.REINDEX_DATA_STREAM_PIPELINE_NAME,
            new BytesArray(customPipeline),
            XContentType.JSON
        );

        safeGet(clusterAdmin().execute(PutPipelineTransportAction.TYPE, putRequest));

        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));

        // add doc with timestamp
        String time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        var doc = String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, time);
        addDoc(sourceIndex, doc);

        // add timestamp to source mapping
        indicesAdmin().preparePutMapping(sourceIndex).setSource(DATA_STREAM_MAPPING, XContentType.JSON).get();

        String destIndex = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        ).getDestIndex();

        assertResponse(prepareSearch(destIndex), response -> {
            Map<String, Object> sourceAsMap = response.getHits().getAt(0).getSourceAsMap();
            assertEquals("gorgonzola", sourceAsMap.get("cheese"));
            assertEquals(time, sourceAsMap.get(DEFAULT_TIMESTAMP_FIELD));
        });
    }

    public void testDestIndexDeletedIfExists() throws Exception {
        // empty source index
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));

        // dest index with docs
        var destIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        safeGet(indicesAdmin().create(new CreateIndexRequest(destIndex)));
        indexDocs(destIndex, 10);
        safeGet(indicesAdmin().refresh(new RefreshRequest(destIndex)));
        assertHitCount(prepareSearch(destIndex).setSize(0), 10);

        // call reindex
        safeGet(client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex)));

        // verify that dest still exists, but is now empty
        assertTrue(indexExists(destIndex));
        assertHitCount(prepareSearch(destIndex).setSize(0), 0);
    }

    public void testDestIndexNameSet_noDotPrefix() throws Exception {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));

        // call reindex
        var response = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        );

        var expectedDestIndexName = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndexName, response.getDestIndex());
    }

    public void testDestIndexNameSet_withDotPrefix() throws Exception {

        var sourceIndex = "." + randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));

        // call reindex
        var response = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        );

        var expectedDestIndexName = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndexName, response.getDestIndex());
    }

    public void testDestIndexContainsDocs() throws Exception {
        // source index with docs
        var numDocs = randomIntBetween(1, 100);
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));
        indexDocs(sourceIndex, numDocs);

        // call reindex
        var response = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        );
        safeGet(indicesAdmin().refresh(new RefreshRequest(response.getDestIndex())));

        // verify that dest contains docs
        assertHitCount(prepareSearch(response.getDestIndex()).setSize(0), numDocs);
    }

    public void testSetSourceToBlockWrites() throws Exception {
        var settings = randomBoolean() ? Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build() : Settings.EMPTY;

        // empty source index
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex, settings)));

        // call reindex
        safeGet(client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex)));

        // Assert that source index is now read-only but not verified read-only
        GetSettingsResponse getSettingsResponse = safeGet(admin().indices().getSettings(new GetSettingsRequest().indices(sourceIndex)));
        assertTrue(parseBoolean(getSettingsResponse.getSetting(sourceIndex, IndexMetadata.SETTING_BLOCKS_WRITE)));
        assertFalse(
            parseBoolean(getSettingsResponse.getSetting(sourceIndex, MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey()))
        );

        // assert that write to source fails
        var indexReq = new IndexRequest(sourceIndex).source(jsonBuilder().startObject().field("field", "1").endObject());
        expectThrows(ClusterBlockException.class, client().index(indexReq));
        assertHitCount(prepareSearch(sourceIndex).setSize(0), 0);
    }

    public void testMissingSourceIndex() {
        var nonExistentSourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        expectThrows(
            ResourceNotFoundException.class,
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(nonExistentSourceIndex))
        );
    }

    public void testSettingsAddedBeforeReindex() throws Exception {
        // start with a static setting
        var numShards = randomIntBetween(1, 10);
        var staticSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex, staticSettings)));

        // update with a dynamic setting
        var numReplicas = randomIntBetween(0, 10);
        var refreshInterval = randomIntBetween(1, 100) + "s";
        var dynamicSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval)
            .build();
        safeGet(indicesAdmin().updateSettings(new UpdateSettingsRequest(dynamicSettings, sourceIndex)));

        // call reindex
        var destIndex = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        ).getDestIndex();

        // assert both static and dynamic settings set on dest index
        var settingsResponse = safeGet(indicesAdmin().getSettings(new GetSettingsRequest().indices(destIndex)));
        assertEquals(numReplicas, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_REPLICAS)));
        assertEquals(numShards, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_SHARDS)));
        assertEquals(refreshInterval, settingsResponse.getSetting(destIndex, IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()));
    }

    public void testMappingsAddedToDestIndex() {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(MAPPING)));

        // call reindex
        var destIndex = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        ).getDestIndex();

        var mappingsResponse = safeGet(
            indicesAdmin().getMappings(new GetMappingsRequest(TEST_REQUEST_TIMEOUT).indices(sourceIndex, destIndex))
        );
        Map<String, MappingMetadata> mappings = mappingsResponse.mappings();
        var destMappings = mappings.get(destIndex).sourceAsMap();
        var sourceMappings = mappings.get(sourceIndex).sourceAsMap();

        assertEquals(sourceMappings, destMappings);
        // sanity check specific value from dest mapping
        assertEquals("text", XContentMapValues.extractValue("properties.foo1.type", destMappings));
    }

    public void testFailIfMetadataBlockSet() {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        var settings = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_METADATA, true).build();
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex, settings)));

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        );
        assertTrue(e.getMessage().contains("Cannot reindex index") || e.getCause().getMessage().equals("Cannot reindex index"));

        cleanupMetadataBlocks(sourceIndex);
    }

    public void testFailIfReadBlockSet() {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        var settings = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_READ, true).build();
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex, settings)));

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        );
        assertTrue(e.getMessage().contains("Cannot reindex index") || e.getCause().getMessage().equals("Cannot reindex index"));

        cleanupMetadataBlocks(sourceIndex);
    }

    public void testReadOnlyBlocksNotAddedBack() {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_READ_ONLY, randomBoolean())
            .put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, randomBoolean())
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, randomBoolean())
            .build();
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex, settings)));

        // call reindex
        var destIndex = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        ).getDestIndex();

        var settingsResponse = safeGet(indicesAdmin().getSettings(new GetSettingsRequest().indices(destIndex)));
        assertFalse(parseBoolean(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_READ_ONLY)));
        assertFalse(parseBoolean(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE)));
        assertFalse(parseBoolean(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_BLOCKS_WRITE)));

        cleanupMetadataBlocks(sourceIndex);
        cleanupMetadataBlocks(destIndex);
    }

    public void testUpdateSettingsDefaultsRestored() {
        // ESIntegTestCase creates a template random_index_template which contains a value for number_of_replicas.
        // Since this test checks the behavior of default settings, there cannot be a value for number_of_replicas,
        // so we delete the template within this method. This has no effect on other tests which will still
        // have the template created during their setup.
        assertAcked(
            indicesAdmin().execute(TransportDeleteIndexTemplateAction.TYPE, new DeleteIndexTemplateRequest("random_index_template"))
        );

        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        assertAcked(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));

        // call reindex
        var destIndex = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        ).getDestIndex();

        var settingsResponse = safeGet(indicesAdmin().getSettings(new GetSettingsRequest().indices(sourceIndex, destIndex)));
        var destSettings = settingsResponse.getIndexToSettings().get(destIndex);

        assertEquals(
            IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getDefault(destSettings),
            IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(destSettings)
        );
        assertEquals(
            IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getDefault(destSettings),
            IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.get(destSettings)
        );
    }

    public void testSettingsAndMappingsFromTemplate() throws IOException {
        var numShards = randomIntBetween(1, 10);
        var numReplicas = randomIntBetween(0, 10);

        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .build();

        // Create template with settings and mappings
        var template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("logs-*"))
            .template(new Template(settings, CompressedXContent.fromJSON(MAPPING), null))
            .build();
        var request = new TransportPutComposableIndexTemplateAction.Request("logs-template");
        request.indexTemplate(template);
        safeGet(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));

        var sourceIndex = "logs-" + randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));

        {
            var indexRequest = new IndexRequest(sourceIndex);
            indexRequest.source("{ \"foo1\": \"cheese\" }", XContentType.JSON);
            safeGet(client().index(indexRequest));
        }

        // call reindex
        var destIndex = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
        ).getDestIndex();

        // verify settings from templates copied to dest index
        {
            var settingsResponse = safeGet(indicesAdmin().getSettings(new GetSettingsRequest().indices(destIndex)));
            assertEquals(numReplicas, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_REPLICAS)));
            assertEquals(numShards, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_SHARDS)));
        }

        // verify mappings from templates copied to dest index
        {
            var mappingsResponse = safeGet(
                indicesAdmin().getMappings(new GetMappingsRequest(TEST_REQUEST_TIMEOUT).indices(sourceIndex, destIndex))
            );
            var destMappings = mappingsResponse.mappings().get(destIndex).sourceAsMap();
            var sourceMappings = mappingsResponse.mappings().get(sourceIndex).sourceAsMap();
            assertEquals(sourceMappings, destMappings);
            // sanity check specific value from dest mapping
            assertEquals("text", XContentMapValues.extractValue("properties.foo1.type", destMappings));
        }

        // verify doc was successfully added
        assertHitCount(prepareSearch(destIndex).setSize(0), 1);
    }

    private static final String TSDB_MAPPING = """
        {
          "_doc":{
            "properties": {
              "@timestamp" : {
                "type": "date"
              },
              "metricset": {
                "type": "keyword",
                "time_series_dimension": true
              }
            }
          }
        }""";

    private static final String TSDB_DOC = """
        {
            "@timestamp": "$time",
            "metricset": "pod",
            "k8s": {
                "pod": {
                    "name": "dog",
                    "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9",
                    "ip": "10.10.55.3",
                    "network": {
                        "tx": 1434595272,
                        "rx": 530605511
                    }
                }
            }
        }
        """;

    public void testTsdbStartEndSet() throws Exception {
        var templateSettings = Settings.builder().put("index.mode", "time_series");
        if (randomBoolean()) {
            templateSettings.put("index.routing_path", "metricset");
        }
        var mapping = new CompressedXContent(TSDB_MAPPING);

        // create template
        var request = new TransportPutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("k8s*"))
                .template(new Template(templateSettings.build(), mapping, null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        safeGet(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));

        // index doc
        Instant time = Instant.now();
        String backingIndexName;
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(TSDB_DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            var indexResponse = safeGet(client().index(indexRequest));
            backingIndexName = indexResponse.getIndex();
        }

        var sourceSettings = safeGet(indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(backingIndexName)))
            .getSettings()
            .get(backingIndexName);
        Instant startTime = IndexSettings.TIME_SERIES_START_TIME.get(sourceSettings);
        Instant endTime = IndexSettings.TIME_SERIES_END_TIME.get(sourceSettings);

        // sanity check start/end time on source
        assertNotNull(startTime);
        assertNotNull(endTime);
        assertTrue(endTime.isAfter(startTime));

        // force a rollover so can call reindex and delete
        var rolloverRequest = new RolloverRequest("k8s", null);
        var rolloverResponse = safeGet(indicesAdmin().rolloverIndex(rolloverRequest));
        rolloverResponse.getNewIndex();

        // call reindex on the original backing index
        var destIndex = safeGet(
            client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(backingIndexName))
        ).getDestIndex();

        var destSettings = safeGet(indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(destIndex))).getSettings()
            .get(destIndex);
        var destStart = IndexSettings.TIME_SERIES_START_TIME.get(destSettings);
        var destEnd = IndexSettings.TIME_SERIES_END_TIME.get(destSettings);

        assertEquals(startTime, destStart);
        assertEquals(endTime, destEnd);

        // verify doc was successfully added
        assertHitCount(prepareSearch(destIndex).setSize(0), 1);
    }

    private static void cleanupMetadataBlocks(String index) {
        var settings = Settings.builder()
            .putNull(IndexMetadata.SETTING_READ_ONLY)
            .putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE)
            .putNull(IndexMetadata.SETTING_BLOCKS_METADATA)
            .build();
        safeGet(indicesAdmin().updateSettings(new UpdateSettingsRequest(settings, index)));
    }

    private static void indexDocs(String index, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .id(i + "")
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = safeGet(client().bulk(bulkRequest));
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
    }

    private static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    void addDoc(String index, String doc) {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(doc, XContentType.JSON));
        safeGet(client().bulk(bulkRequest));
    }
}
