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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.ReindexDataStreamIndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.datastreams.action.TransportReindexDataStreamIndexAction;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDatastreamIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, ReindexPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testDestIndexDeletedIfExists() throws Exception {
        // empty source index
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // dest index with docs
        var destIndex = TransportReindexDataStreamIndexAction.generateDestIndexName(sourceIndex);
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
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // call reindex
        var response = client().execute(ReindexDataStreamIndexAction.INSTANCE, new ReindexDataStreamIndexAction.Request(sourceIndex))
            .actionGet();

        var expectedDestIndexName = TransportReindexDataStreamIndexAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndexName, response.getDestIndex());
    }

    public void testDestIndexContainsDocs() throws Exception {
        // empty source index
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

    // TODO error on set read-only if don't have perms
    // TODO check settings added after
    // TODO check tsdb start/end time correct
    // TODO check other metadata
    // TODO check logsdb/tsdb work
    // TODO settings and mappings that come from template
    // TODO test that does not fail on create if index exists after delete (Not sure how to do this since relies on race condition)

    static void indexDocs(String index, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        indicesAdmin().refresh(new RefreshRequest(index)).actionGet();
    }
}
