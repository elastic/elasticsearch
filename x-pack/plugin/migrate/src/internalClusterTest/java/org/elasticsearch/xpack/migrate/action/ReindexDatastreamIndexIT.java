/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.migrate.MigratePlugin;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.REINDEX_DATA_STREAM_FEATURE_FLAG;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDatastreamIndexIT extends ESIntegTestCase {

    private static final String MAPPING = """
        {
          "_doc":{
            "dynamic":"strict",
            "properties":{
              "foo1":{
                "type":"text"
              }
            }
          }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MigratePlugin.class, ReindexPlugin.class, MockTransportService.TestPlugin.class, DataStreamsPlugin.class);
    }

    public void testDestIndexDeletedIfExists() throws Exception {
        assumeTrue("requires the migration reindex feature flag", REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled());

        // empty source index
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // dest index with docs
        var destIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        indicesAdmin().create(new CreateIndexRequest(destIndex)).actionGet();
        indexDocs(destIndex, 10);
        assertHitCount(prepareSearch(destIndex).setSize(0), 10);

        // call reindex
        client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex)).actionGet();

        // verify that dest still exists, but is now empty
        assertTrue(indexExists(destIndex));
        assertHitCount(prepareSearch(destIndex).setSize(0), 0);
    }

    public void testDestIndexNameSet() throws Exception {
        assumeTrue("requires the migration reindex feature flag", REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled());

        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // call reindex
        var response = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
            .actionGet();

        var expectedDestIndexName = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndexName, response.getDestIndex());
    }

    public void testDestIndexContainsDocs() throws Exception {
        assumeTrue("requires the migration reindex feature flag", REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled());

        // source index with docs
        var numDocs = randomIntBetween(1, 100);
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();
        indexDocs(sourceIndex, numDocs);

        // call reindex
        var response = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
            .actionGet();
        indicesAdmin().refresh(new RefreshRequest(response.getDestIndex())).actionGet();

        // verify that dest contains docs
        assertHitCount(prepareSearch(response.getDestIndex()).setSize(0), numDocs);
    }

    public void testSetSourceToReadOnly() throws Exception {
        assumeTrue("requires the migration reindex feature flag", REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled());

        // empty source index
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // call reindex
        client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex)).actionGet();

        // assert that write to source fails
        var indexReq = new IndexRequest(sourceIndex).source(jsonBuilder().startObject().field("field", "1").endObject());
        assertThrows(ClusterBlockException.class, () -> client().index(indexReq).actionGet());
        assertHitCount(prepareSearch(sourceIndex).setSize(0), 0);
    }

    public void testSettingsAddedBeforeReindex() throws Exception {
        assumeTrue("requires the migration reindex feature flag", REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled());

        // start with a static setting
        var numShards = randomIntBetween(1, 10);
        var staticSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex, staticSettings)).get();

        // update with a dynamic setting
        var numReplicas = randomIntBetween(0, 10);
        var dynamicSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas).build();
        indicesAdmin().updateSettings(new UpdateSettingsRequest(dynamicSettings, sourceIndex)).actionGet();

        // call reindex
        var destIndex = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
            .actionGet()
            .getDestIndex();

        // assert both static and dynamic settings set on dest index
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest().indices(destIndex)).actionGet();
        assertEquals(numReplicas, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_REPLICAS)));
        assertEquals(numShards, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_SHARDS)));
    }

    public void testMappingsAddedToDestIndex() throws Exception {
        assumeTrue("requires the migration reindex feature flag", REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled());

        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        String mapping = """
            {
              "_doc":{
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"text"
                  }
                }
              }
            }
            """;
        indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mapping)).actionGet();

        // call reindex
        var destIndex = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
            .actionGet()
            .getDestIndex();

        var mappingsResponse = indicesAdmin().getMappings(new GetMappingsRequest().indices(sourceIndex, destIndex)).actionGet();
        Map<String, MappingMetadata> mappings = mappingsResponse.mappings();
        var destMappings = mappings.get(destIndex).sourceAsMap();
        var sourceMappings = mappings.get(sourceIndex).sourceAsMap();

        assertEquals(sourceMappings, destMappings);
        // sanity check specific value from dest mapping
        assertEquals("text", XContentMapValues.extractValue("properties.foo1.type", destMappings));
    }

    public void testReadOnlyAddedBack() {
        assumeTrue("requires the migration reindex feature flag", REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled());

        // Create source index with read-only and/or block-writes
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        boolean isReadOnly = randomBoolean();
        boolean isBlockWrites = randomBoolean();
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_READ_ONLY, isReadOnly)
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, isBlockWrites)
            .build();
        indicesAdmin().create(new CreateIndexRequest(sourceIndex, settings)).actionGet();

        // call reindex
        var destIndex = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
            .actionGet()
            .getDestIndex();

        // assert read-only settings added back to dest index
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest().indices(destIndex)).actionGet();
        assertEquals(isReadOnly, Boolean.parseBoolean(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_READ_ONLY)));
        assertEquals(isBlockWrites, Boolean.parseBoolean(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_BLOCKS_WRITE)));

        removeReadOnly(sourceIndex);
        removeReadOnly(destIndex);
    }

    public void testSettingsAndMappingsFromTemplate() throws IOException {
        assumeTrue("requires the migration reindex feature flag", REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled());

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
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        var sourceIndex = "logs-" + randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).actionGet();

        // call reindex
        var destIndex = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
            .actionGet()
            .getDestIndex();

        // verify settings from templates copied to dest index
        {
            var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest().indices(destIndex)).actionGet();
            assertEquals(numReplicas, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_REPLICAS)));
            assertEquals(numShards, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_SHARDS)));
        }

        // verify mappings from templates copied to dest index
        {
            var mappingsResponse = indicesAdmin().getMappings(new GetMappingsRequest().indices(sourceIndex, destIndex)).actionGet();
            var destMappings = mappingsResponse.mappings().get(destIndex).sourceAsMap();
            var sourceMappings = mappingsResponse.mappings().get(sourceIndex).sourceAsMap();
            assertEquals(sourceMappings, destMappings);
            // sanity check specific value from dest mapping
            assertEquals("text", XContentMapValues.extractValue("properties.foo1.type", destMappings));
        }
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
        assumeTrue("requires the migration reindex feature flag", REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled());

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
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        // index doc
        Instant time = Instant.now();
        String backingIndexName;
        {
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(TSDB_DOC.replace("$time", formatInstant(time)), XContentType.JSON);
            var indexResponse = client().index(indexRequest).actionGet();
            backingIndexName = indexResponse.getIndex();
        }

        var sourceSettings = indicesAdmin().getIndex(new GetIndexRequest().indices(backingIndexName))
            .actionGet()
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
        var rolloverResponse = indicesAdmin().rolloverIndex(rolloverRequest).actionGet();
        rolloverResponse.getNewIndex();

        // call reindex on the original backing index
        var destIndex = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(backingIndexName))
            .actionGet()
            .getDestIndex();

        var destSettings = indicesAdmin().getIndex(new GetIndexRequest().indices(destIndex)).actionGet().getSettings().get(destIndex);
        var destStart = IndexSettings.TIME_SERIES_START_TIME.get(destSettings);
        var destEnd = IndexSettings.TIME_SERIES_END_TIME.get(destSettings);

        assertEquals(startTime, destStart);
        assertEquals(endTime, destEnd);
    }

    // TODO more logsdb/tsdb specific tests
    // TODO more data stream specific tests (how are data streams indices are different from regular indices?)
    // TODO check other IndexMetadata fields that need to be fixed after the fact
    // TODO what happens if don't have necessary perms for a given index?

    private static void removeReadOnly(String index) {
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_READ_ONLY, false)
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, false)
            .build();
        assertAcked(indicesAdmin().updateSettings(new UpdateSettingsRequest(settings, index)).actionGet());
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
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        indicesAdmin().refresh(new RefreshRequest(index)).actionGet();
    }

    private static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    private static String getIndexUUID(String index) {
        return indicesAdmin().getIndex(new GetIndexRequest().indices(index))
            .actionGet()
            .getSettings()
            .get(index)
            .get(IndexMetadata.SETTING_INDEX_UUID);
    }
}
