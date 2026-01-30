/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.mapping;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class UpdateMappingIntegrationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testDynamicUpdates() throws Exception {
        indicesAdmin().prepareCreate("test")
            .setSettings(indexSettings(1, 0).put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE))
            .get();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();
        updateClusterSettings(
            Settings.builder().put(MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMinutes(5))
        );

        int recCount = randomIntBetween(20, 200);
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int rec = 0; rec < recCount; rec++) {
            String type = "type";
            String fieldName = "field_" + type + "_" + rec;
            indexRequests.add(
                prepareIndex("test").setId(Integer.toString(rec))
                    .setTimeout(TimeValue.timeValueMinutes(5))
                    .setSource(fieldName, "some_value")
            );
        }
        indexRandom(true, false, indexRequests);

        logger.info("checking all the documents are there");
        BroadcastResponse refreshResponse = indicesAdmin().prepareRefresh().get();
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        assertHitCount(prepareSearch("test").setSize(0), recCount);

        logger.info("checking all the fields are in the mappings");

        for (int rec = 0; rec < recCount; rec++) {
            String type = "type";
            String fieldName = "field_" + type + "_" + rec;
            assertConcreteMappingsOnAll("test", fieldName);
        }

        updateClusterSettings(Settings.builder().putNull(MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.getKey()));
    }

    public void testUpdateMappingWithoutType() {
        indicesAdmin().prepareCreate("test").setSettings(indexSettings(1, 0)).setMapping("""
            {"properties":{"body":{"type":"text"}}}
            """).get();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        AcknowledgedResponse putMappingResponse = indicesAdmin().preparePutMapping("test").setSource("""
            {"properties":{"date":{"type":"integer"}}}
            """, XContentType.JSON).get();

        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, "test").get();
        assertThat(getMappingsResponse.mappings().get("test").source().toString(), equalTo("""
            {"_doc":{"properties":{"body":{"type":"text"},"date":{"type":"integer"}}}}"""));
    }

    public void testUpdateMappingWithoutTypeMultiObjects() {
        createIndex("test", 1, 0);
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        AcknowledgedResponse putMappingResponse = indicesAdmin().preparePutMapping("test").setSource("""
            {"properties":{"date":{"type":"integer"}}}""", XContentType.JSON).get();

        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, "test").get();
        assertThat(getMappingsResponse.mappings().get("test").source().toString(), equalTo("""
            {"_doc":{"properties":{"date":{"type":"integer"}}}}"""));
    }

    public void testUpdateMappingWithConflicts() {
        indicesAdmin().prepareCreate("test").setSettings(indexSettings(2, 0)).setMapping("""
            {"properties":{"body":{"type":"text"}}}
            """).get();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        try {
            indicesAdmin().preparePutMapping("test").setSource("""
                {"_doc":{"properties":{"body":{"type":"integer"}}}}
                """, XContentType.JSON).get();
            fail("Expected MergeMappingException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [body] cannot be changed from type [text] to [integer]"));
        }
    }

    public void testUpdateMappingWithNormsConflicts() {
        indicesAdmin().prepareCreate("test").setMapping("""
            {"properties":{"body":{"type":"text", "norms": false }}}
            """).get();
        try {
            indicesAdmin().preparePutMapping("test").setSource("""
                {"_doc":{"properties":{"body":{"type":"text", "norms": true }}}}
                """, XContentType.JSON).get();
            fail("Expected MergeMappingException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot update parameter [norms] from [false] to [true]"));
        }
    }

    /*
    Second regression test for https://github.com/elastic/elasticsearch/issues/3381
     */
    public void testUpdateMappingNoChanges() {
        indicesAdmin().prepareCreate("test").setSettings(indexSettings(2, 0)).setMapping("""
            {"properties":{"body":{"type":"text"}}}""").get();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        AcknowledgedResponse putMappingResponse = indicesAdmin().preparePutMapping("test").setSource("""
            {"_doc":{"properties":{"body":{"type":"text"}}}}
            """, XContentType.JSON).get();

        // no changes, we return
        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));
    }

    public void testUpdateMappingConcurrently() throws Throwable {
        createIndex("test1", "test2");

        final AtomicReference<Exception> threadException = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ArrayList<Client> clientArray = new ArrayList<>();
        for (Client c : clients()) {
            clientArray.add(c);
        }

        startInParallel(3, j -> {
            try {
                for (int i = 0; i < 100; i++) {
                    if (stop.get()) {
                        return;
                    }

                    Client client1 = clientArray.get(i % clientArray.size());
                    Client client2 = clientArray.get((i + 1) % clientArray.size());
                    String indexName = i % 2 == 0 ? "test2" : "test1";
                    String fieldName = "t_" + j + "_" + i;

                    AcknowledgedResponse response = client1.admin()
                        .indices()
                        .preparePutMapping(indexName)
                        .setSource(
                            JsonXContent.contentBuilder()
                                .startObject()
                                .startObject("_doc")
                                .startObject("properties")
                                .startObject(fieldName)
                                .field("type", "text")
                                .endObject()
                                .endObject()
                                .endObject()
                                .endObject()
                        )
                        .setMasterNodeTimeout(TimeValue.timeValueMinutes(5))
                        .get();

                    assertThat(response.isAcknowledged(), equalTo(true));
                    GetMappingsResponse getMappingResponse = client2.admin()
                        .indices()
                        .prepareGetMappings(TEST_REQUEST_TIMEOUT, indexName)
                        .get();
                    MappingMetadata mappings = getMappingResponse.getMappings().get(indexName);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> properties = (Map<String, Object>) mappings.getSourceAsMap().get("properties");
                    assertThat(properties.keySet(), Matchers.hasItem(fieldName));
                }
            } catch (Exception e) {
                threadException.set(e);
                stop.set(true);
            }
        });

        if (threadException.get() != null) {
            throw threadException.get();
        }

    }

    public void testPutMappingsWithBlocks() {
        createIndex("test");
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", block);
                assertAcked(indicesAdmin().preparePutMapping("test").setSource("""
                    {"properties":{"date":{"type":"integer"}}}
                    """, XContentType.JSON));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        for (String block : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", block);
                assertBlocked(indicesAdmin().preparePutMapping("test").setSource("""
                    {"properties":{"date":{"type":"integer"}}}
                    """, XContentType.JSON));
            } finally {
                disableIndexBlock("test", block);
            }
        }
    }

    /**
     * Waits until mappings for the provided fields exist on all nodes. Note, this waits for the current
     * started shards and checks for concrete mappings.
     */
    private void assertConcreteMappingsOnAll(final String index, final String... fieldNames) {
        Set<String> nodes = internalCluster().nodesInclude(index);
        assertThat(nodes, Matchers.not(Matchers.emptyIterable()));
        for (String node : nodes) {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(resolveIndex(index));
            assertThat("index service doesn't exists on " + node, indexService, notNullValue());
            MapperService mapperService = indexService.mapperService();
            for (String fieldName : fieldNames) {
                MappedFieldType fieldType = mapperService.fieldType(fieldName);
                assertNotNull("field " + fieldName + " doesn't exists on " + node, fieldType);
            }
        }
        assertMappingOnMaster(index, fieldNames);
    }

    /**
     * Waits for the given mapping type to exists on the master node.
     */
    private void assertMappingOnMaster(final String index, final String... fieldNames) {
        GetMappingsResponse response = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get();
        MappingMetadata mappings = response.getMappings().get(index);
        assertThat(mappings, notNullValue());

        Map<String, Object> mappingSource = mappings.getSourceAsMap();
        assertFalse(mappingSource.isEmpty());
        assertTrue(mappingSource.containsKey("properties"));

        for (String fieldName : fieldNames) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mappingProperties = (Map<String, Object>) mappingSource.get("properties");
            if (fieldName.indexOf('.') != -1) {
                fieldName = fieldName.replace(".", ".properties.");
            }
            assertThat(
                "field " + fieldName + " doesn't exists in mapping " + mappings.source().string(),
                XContentMapValues.extractValue(fieldName, mappingProperties),
                notNullValue()
            );
        }
    }
}
