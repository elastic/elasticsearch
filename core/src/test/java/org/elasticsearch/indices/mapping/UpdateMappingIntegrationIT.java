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

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

@ClusterScope(randomDynamicTemplates = false)
public class UpdateMappingIntegrationIT extends ESIntegTestCase {

    public void testDynamicUpdates() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
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
        assertThat(response.getHits().totalHits(), equalTo((long) recCount));

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
                        settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).addMapping("doc", "{\"doc\":{\"properties\":{\"body\":{\"type\":\"string\"}}}}")
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("doc")
                .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}")
                .execute().actionGet();

        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").execute().actionGet();
        assertThat(getMappingsResponse.mappings().get("test").get("doc").source().toString(),
                equalTo("{\"doc\":{\"properties\":{\"body\":{\"type\":\"string\"},\"date\":{\"type\":\"integer\"}}}}"));
    }

    public void testUpdateMappingWithoutTypeMultiObjects() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("doc")
                .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}")
                .execute().actionGet();

        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").execute().actionGet();
        assertThat(getMappingsResponse.mappings().get("test").get("doc").source().toString(),
                equalTo("{\"doc\":{\"properties\":{\"date\":{\"type\":\"integer\"}}}}"));
    }

    public void testUpdateMappingWithConflicts() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).addMapping("type", "{\"type\":{\"properties\":{\"body\":{\"type\":\"string\"}}}}")
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        try {
            client().admin().indices().preparePutMapping("test").setType("type")
                    .setSource("{\"type\":{\"properties\":{\"body\":{\"type\":\"integer\"}}}}").execute().actionGet();
            fail("Expected MergeMappingException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [body] of different type, current_type [string], merged_type [integer]"));
        }
    }

    public void testUpdateMappingWithNormsConflicts() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("type", "{\"type\":{\"properties\":{\"body\":{\"type\":\"string\", \"norms\": { \"enabled\": false }}}}}")
                .execute().actionGet();
        try {
            client().admin().indices().preparePutMapping("test").setType("type")
                    .setSource("{\"type\":{\"properties\":{\"body\":{\"type\":\"string\", \"norms\": { \"enabled\": true }}}}}").execute()
                    .actionGet();
            fail("Expected MergeMappingException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [body] has different [omit_norms]"));
        }
    }

    /*
    Second regression test for https://github.com/elasticsearch/elasticsearch/issues/3381
     */
    public void testUpdateMappingNoChanges() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).addMapping("type", "{\"type\":{\"properties\":{\"body\":{\"type\":\"string\"}}}}")
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("type")
                .setSource("{\"type\":{\"properties\":{\"body\":{\"type\":\"string\"}}}}")
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
                        .startObject("properties").startObject("f").field("type", "string").field("index", "analyzed").endObject().endObject()
                        .endObject().endObject()
        ).get();
        assertThat(putResponse.isAcknowledged(), equalTo(true));


        logger.info("Changing _default_ mappings field from analyzed to non-analyzed");
        putResponse = client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .startObject("properties").startObject("f").field("type", "string").field("index", "not_analyzed").endObject().endObject()
                        .endObject().endObject()
        ).get();
        assertThat(putResponse.isAcknowledged(), equalTo(true));
        logger.info("Done changing _default_ mappings field from analyzed to non-analyzed");

        getResponse = client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
        defaultMapping = getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
        Map<String, Object> fieldSettings = (Map<String, Object>) ((Map) defaultMapping.get("properties")).get("f");
        assertThat(fieldSettings, hasEntry("index", (Object) "not_analyzed"));

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

        // This is important. The test assumes all nodes are aware of all indices. Due to initializing shard throttling
        // not all shards are allocated with the initial create index. Wait for it..
        ensureYellow();

        final Throwable[] threadException = new Throwable[1];
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
                            String typeName = "type" + (i % 10);
                            String fieldName = Thread.currentThread().getName() + "_" + i;

                            PutMappingResponse response = client1.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(
                                    JsonXContent.contentBuilder().startObject().startObject(typeName)
                                            .startObject("properties").startObject(fieldName).field("type", "string").endObject().endObject()
                                            .endObject().endObject()
                            ).get();

                            assertThat(response.isAcknowledged(), equalTo(true));
                            GetMappingsResponse getMappingResponse = client2.admin().indices().prepareGetMappings(indexName).get();
                            ImmutableOpenMap<String, MappingMetaData> mappings = getMappingResponse.getMappings().get(indexName);
                            assertThat(mappings.containsKey(typeName), equalTo(true));
                            assertThat(((Map<String, Object>) mappings.get(typeName).getSourceAsMap().get("properties")).keySet(), Matchers.hasItem(fieldName));
                        }
                    } catch (Throwable t) {
                        threadException[0] = t;
                        stop.set(true);
                    }
                }
            });

            threads[j].setName("t_" + j);
            threads[j].start();
        }

        for (Thread t : threads) t.join();

        if (threadException[0] != null) {
            throw threadException[0];
        }

    }

    public void testPutMappingsWithBlocks() throws Exception {
        createIndex("test");
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", block);
                assertAcked(client().admin().indices().preparePutMapping("test").setType("doc").setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}"));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        for (String block : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", block);
                assertBlocked(client().admin().indices().preparePutMapping("test").setType("doc").setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}"));
            } finally {
                disableIndexBlock("test", block);
            }
        }
    }
}
