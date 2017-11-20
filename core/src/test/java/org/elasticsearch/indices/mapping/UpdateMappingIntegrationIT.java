/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.mapping;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class UpdateMappingIntegrationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testDynamicUpdates() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        Settings.builder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE)
                                .put("index.version.created", Version.V_5_6_0) // for multiple types
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        int recCount = randomIntBetween(200, 600);
        int numberOfTypes = randomIntBetween(1, 5);
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int rec = 0; rec < recCount; rec++) {
            String type = "type" + (rec % numberOfTypes);
            String fieldName = "field_" + type + "_" + rec;
            indexRequests.add(client().prepareIndex("test", type, Integer.toString(rec)).setSource(fieldName, "some_value"));
        }
        indexRandom(true, indexRequests);

        logger.info("checking all the documents are there");
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        SearchResponse response = client().prepareSearch("test").setSize(0).execute().actionGet();
        assertThat(response.getHits().getTotalHits(), equalTo((long) recCount));

        logger.info("checking all the fields are in the mappings");

        for (int rec = 0; rec < recCount; rec++) {
            String type = "type" + (rec % numberOfTypes);
            String fieldName = "field_" + type + "_" + rec;
            assertConcreteMappingsOnAll("test", type, fieldName);
        }
    }

    public void testUpdateMappingWithoutType() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        Settings.builder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).addMapping("doc", "{\"doc\":{\"properties\":{\"body\":{\"type\":\"text\"}}}}", XContentType.JSON)
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("doc")
                .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}", XContentType.JSON)
                .execute().actionGet();

        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").execute().actionGet();
        assertThat(getMappingsResponse.mappings().get("test").get("doc").source().toString(),
                equalTo("{\"doc\":{\"properties\":{\"body\":{\"type\":\"text\"},\"date\":{\"type\":\"integer\"}}}}"));
    }

    public void testUpdateMappingWithoutTypeMultiObjects() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        Settings.builder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("doc")
                .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}", XContentType.JSON)
                .execute().actionGet();

        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").execute().actionGet();
        assertThat(getMappingsResponse.mappings().get("test").get("doc").source().toString(),
                equalTo("{\"doc\":{\"properties\":{\"date\":{\"type\":\"integer\"}}}}"));
    }

    public void testUpdateMappingWithConflicts() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        Settings.builder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).addMapping("type", "{\"type\":{\"properties\":{\"body\":{\"type\":\"text\"}}}}", XContentType.JSON)
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        try {
            client().admin().indices().preparePutMapping("test").setType("type")
                    .setSource("{\"type\":{\"properties\":{\"body\":{\"type\":\"integer\"}}}}", XContentType.JSON).execute().actionGet();
            fail("Expected MergeMappingException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [body] of different type, current_type [text], merged_type [integer]"));
        }
    }

    public void testUpdateMappingWithNormsConflicts() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("type", "{\"type\":{\"properties\":{\"body\":{\"type\":\"text\", \"norms\": false }}}}", XContentType.JSON)
                .execute().actionGet();
        try {
            client().admin().indices().preparePutMapping("test").setType("type")
                    .setSource("{\"type\":{\"properties\":{\"body\":{\"type\":\"text\", \"norms\": true }}}}", XContentType.JSON).execute()
                    .actionGet();
            fail("Expected MergeMappingException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [body] has different [norms]"));
        }
    }

    /*
    Second regression test for https://github.com/elastic/elasticsearch/issues/3381
     */
    public void testUpdateMappingNoChanges() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        Settings.builder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).addMapping("type", "{\"type\":{\"properties\":{\"body\":{\"type\":\"text\"}}}}", XContentType.JSON)
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("type")
                .setSource("{\"type\":{\"properties\":{\"body\":{\"type\":\"text\"}}}}", XContentType.JSON)
                .execute().actionGet();

        //no changes, we return
        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDefaultMappingSettings() throws Exception {
        logger.info("Creating index with _default_ mappings");
        client().admin().indices().prepareCreate("test").addMapping(MapperService.DEFAULT_MAPPING,
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .field("date_detection", false)
                        .endObject().endObject()
        ).get();

        GetMappingsResponse getResponse = client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
        Map<String, Object> defaultMapping = getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
        assertThat(defaultMapping, hasKey("date_detection"));


        logger.info("Emptying _default_ mappings");
        // now remove it
        PutMappingResponse putResponse = client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .endObject().endObject()
        ).get();
        assertThat(putResponse.isAcknowledged(), equalTo(true));
        logger.info("Done Emptying _default_ mappings");

        getResponse = client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
        defaultMapping = getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
        assertThat(defaultMapping, not(hasKey("date_detection")));

        // now test you can change stuff that are normally unchangeable
        logger.info("Creating _default_ mappings with an analyzed field");
        putResponse = client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .startObject("properties").startObject("f").field("type", "text").field("index", true).endObject().endObject()
                        .endObject().endObject()
        ).get();
        assertThat(putResponse.isAcknowledged(), equalTo(true));


        logger.info("Changing _default_ mappings field from analyzed to non-analyzed");
        putResponse = client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .startObject("properties").startObject("f").field("type", "keyword").endObject().endObject()
                        .endObject().endObject()
        ).get();
        assertThat(putResponse.isAcknowledged(), equalTo(true));
        logger.info("Done changing _default_ mappings field from analyzed to non-analyzed");

        getResponse = client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
        defaultMapping = getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
        Map<String, Object> fieldSettings = (Map<String, Object>) ((Map) defaultMapping.get("properties")).get("f");
        assertThat(fieldSettings, hasEntry("type", (Object) "keyword"));

        // but we still validate the _default_ type
        logger.info("Confirming _default_ mappings validation");
        assertThrows(client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .startObject("properties").startObject("f").field("type", "DOESNT_EXIST").endObject().endObject()
                        .endObject().endObject()
        ), MapperParsingException.class);

    }

    public void testUpdateMappingConcurrently() throws Throwable {
        createIndex("test1", "test2");

        final AtomicReference<Exception> threadException = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] threads = new Thread[3];
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final ArrayList<Client> clientArray = new ArrayList<>();
        for (Client c : clients()) {
            clientArray.add(c);
        }

        for (int j = 0; j < threads.length; j++) {
            threads[j] = new Thread(new Runnable() {
                @SuppressWarnings("unchecked")
                @Override
                public void run() {
                    try {
                        barrier.await();

                        for (int i = 0; i < 100; i++) {
                            if (stop.get()) {
                                return;
                            }

                            Client client1 = clientArray.get(i % clientArray.size());
                            Client client2 = clientArray.get((i + 1) % clientArray.size());
                            String indexName = i % 2 == 0 ? "test2" : "test1";
                            String typeName = "type";
                            String fieldName = Thread.currentThread().getName() + "_" + i;

                            PutMappingResponse response = client1.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(
                                    JsonXContent.contentBuilder().startObject().startObject(typeName)
                                            .startObject("properties").startObject(fieldName).field("type", "text").endObject().endObject()
                                            .endObject().endObject()
                            ).get();

                            assertThat(response.isAcknowledged(), equalTo(true));
                            GetMappingsResponse getMappingResponse = client2.admin().indices().prepareGetMappings(indexName).get();
                            ImmutableOpenMap<String, MappingMetaData> mappings = getMappingResponse.getMappings().get(indexName);
                            assertThat(mappings.containsKey(typeName), equalTo(true));
                            assertThat(((Map<String, Object>) mappings.get(typeName).getSourceAsMap().get("properties")).keySet(), Matchers.hasItem(fieldName));
                        }
                    } catch (Exception e) {
                        threadException.set(e);
                        stop.set(true);
                    }
                }
            });

            threads[j].setName("t_" + j);
            threads[j].start();
        }

        for (Thread t : threads) t.join();

        if (threadException.get() != null) {
            throw threadException.get();
        }

    }

    public void testPutMappingsWithBlocks() throws Exception {
        createIndex("test");
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", block);
                assertAcked(client().admin().indices().preparePutMapping("test").setType("doc")
                    .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}", XContentType.JSON));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        for (String block : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", block);
                assertBlocked(client().admin().indices().preparePutMapping("test").setType("doc")
                    .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}", XContentType.JSON));
            } finally {
                disableIndexBlock("test", block);
            }
        }
    }

    public void testUpdateMappingOnAllTypes() throws IOException {
        assertTrue("remove this multi type test", Version.CURRENT.before(Version.fromString("7.0.0")));
        assertAcked(prepareCreate("index")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("type1", "f", "type=keyword").addMapping("type2", "f", "type=keyword"));

        assertAcked(client().admin().indices().preparePutMapping("index")
                .setType("type1")
                .setUpdateAllTypes(true)
                .setSource("f", "type=keyword,null_value=n/a")
                .get());

        GetMappingsResponse mappings = client().admin().indices().prepareGetMappings("index").setTypes("type2").get();
        MappingMetaData type2Mapping = mappings.getMappings().get("index").get("type2").get();
        Map<String, Object> properties = (Map<String, Object>) type2Mapping.sourceAsMap().get("properties");
        Map<String, Object> f = (Map<String, Object>) properties.get("f");
        assertEquals("n/a", f.get("null_value"));
    }
}
