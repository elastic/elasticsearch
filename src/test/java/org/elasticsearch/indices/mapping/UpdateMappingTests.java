package org.elasticsearch.indices.mapping;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.*;

public class UpdateMappingTests extends AbstractIntegrationTest {

    @Test
    public void dynamicUpdates() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        long recCount = 20;
        for (int rec = 0; rec < recCount; rec++) {
            client().prepareIndex("test", "type", "rec" + rec).setSource("field" + rec, "some_value").execute().actionGet();
        }
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        logger.info("Searching");
        CountResponse response = client().prepareCount("test").execute().actionGet();
        assertThat(response.getCount(), equalTo(recCount));
    }

    @Test(expected = MergeMappingException.class)
    public void updateMappingWithConflicts() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).addMapping("type", "{\"type\":{\"properties\":{\"body\":{\"type\":\"string\"}}}}")
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("type")
                .setSource("{\"type\":{\"properties\":{\"body\":{\"type\":\"integer\"}}}}")
                .execute().actionGet();

        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));
    }

    /*
    First regression test for https://github.com/elasticsearch/elasticsearch/issues/3381
     */
    @Test
    public void updateMappingWithIgnoredConflicts() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).addMapping("type", "{\"type\":{\"properties\":{\"body\":{\"type\":\"string\"}}}}")
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("type")
                .setSource("{\"type\":{\"properties\":{\"body\":{\"type\":\"integer\"}}}}")
                .setIgnoreConflicts(true)
                .execute().actionGet();

        //no changes since the only one had a conflict and was ignored, we return
        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));
    }

    /*
    Second regression test for https://github.com/elasticsearch/elasticsearch/issues/3381
     */
    @Test
    public void updateMappingNoChanges() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
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
    @Test
    public void updateIncludeExclude() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", 
                jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("normal").field("type", "long").endObject()
                    .startObject("exclude").field("type", "long").endObject()
                    .startObject("include").field("type", "long").endObject()
                    .endObject().endObject().endObject()));
        ensureGreen(); // make sure that replicas are initialized so the refresh command will work them too

        logger.info("Index doc");
        index("test", "type", "1", JsonXContent.contentBuilder().startObject()
                .field("normal", 1).field("exclude", 1).field("include", 1)
                .endObject()
        );
        refresh(); // commit it for later testing.


        logger.info("Adding exclude settings");
        PutMappingResponse putResponse = client().admin().indices().preparePutMapping("test").setType("type").setSource(
                JsonXContent.contentBuilder().startObject().startObject("type")
                        .startObject("_source")
                        .startArray("excludes").value("exclude").endArray()
                        .endObject().endObject()
        ).get();

        assertTrue(putResponse.isAcknowledged());

        // changed mapping doesn't affect indexed documents (checking backward compatibility)
        GetResponse getResponse = client().prepareGet("test", "type", "1").setRealtime(false).get();
        assertThat(getResponse.getSource(), hasKey("normal"));
        assertThat(getResponse.getSource(), hasKey("exclude"));
        assertThat(getResponse.getSource(), hasKey("include"));


        logger.info("Index doc again");
        index("test", "type", "1", JsonXContent.contentBuilder().startObject()
                .field("normal", 2).field("exclude", 1).field("include", 2)
                .endObject()
        );

        // but do affect newly indexed docs
        getResponse = get("test", "type", "1");
        assertThat(getResponse.getSource(), hasKey("normal"));
        assertThat(getResponse.getSource(), not(hasKey("exclude")));
        assertThat(getResponse.getSource(), hasKey("include"));


        logger.info("Changing mapping to includes");
        putResponse = client().admin().indices().preparePutMapping("test").setType("type").setSource(
                JsonXContent.contentBuilder().startObject().startObject("type")
                        .startObject("_source")
                        .startArray("excludes").endArray()
                        .startArray("includes").value("include").endArray()
                        .endObject().endObject()
        ).get();
        assertTrue(putResponse.isAcknowledged());

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        MappingMetaData typeMapping = getMappingsResponse.getMappings().get("test").get("type");
        assertThat((Map<String, Object>) typeMapping.getSourceAsMap().get("_source"), hasKey("includes"));
        ArrayList<String> includes = (ArrayList<String>) ((Map<String, Object>) typeMapping.getSourceAsMap().get("_source")).get("includes");
        assertThat(includes, contains("include"));
        assertThat((Map<String, Object>) typeMapping.getSourceAsMap().get("_source"), hasKey("excludes"));
        assertThat((ArrayList<String>) ((Map<String, Object>) typeMapping.getSourceAsMap().get("_source")).get("excludes"), emptyIterable());


        logger.info("Indexing doc yet again");
        index("test", "type", "1", JsonXContent.contentBuilder().startObject()
                .field("normal", 3).field("exclude", 3).field("include", 3)
                .endObject()
        );

        getResponse = get("test", "type", "1");
        assertThat(getResponse.getSource(), not(hasKey("normal")));
        assertThat(getResponse.getSource(), not(hasKey("exclude")));
        assertThat(getResponse.getSource(), hasKey("include"));


        logger.info("Adding excludes, but keep includes");
        putResponse = client().admin().indices().preparePutMapping("test").setType("type").setSource(
                JsonXContent.contentBuilder().startObject().startObject("type")
                        .startObject("_source")
                        .startArray("excludes").value("*.excludes").endArray()
                        .endObject().endObject()
        ).get();
        assertTrue(putResponse.isAcknowledged());

        getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        typeMapping = getMappingsResponse.getMappings().get("test").get("type");
        assertThat((Map<String, Object>) typeMapping.getSourceAsMap().get("_source"), hasKey("includes"));
        includes = (ArrayList<String>) ((Map<String, Object>) typeMapping.getSourceAsMap().get("_source")).get("includes");
        assertThat(includes, contains("include"));
        assertThat((Map<String, Object>) typeMapping.getSourceAsMap().get("_source"), hasKey("excludes"));
        ArrayList<String> excludes = (ArrayList<String>) ((Map<String, Object>) typeMapping.getSourceAsMap().get("_source")).get("excludes");
        assertThat(excludes, contains("*.excludes"));


    }

    @SuppressWarnings("unchecked")
    @Test
    public void updateDefaultMappingSettings() throws Exception {

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

        // now test you can change stuff that are normally unchangable
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

    @Test
    public void updateMappingConcurrently() throws Throwable {
        // Test that we can concurrently update different indexes and types.
        int shardNo = Math.max(5, cluster().numNodes());

        prepareCreate("test1").setSettings("index.number_of_shards", shardNo).execute().actionGet();
        prepareCreate("test2").setSettings("index.number_of_shards", shardNo).execute().actionGet();

        // This is important. The test assumes all nodes are aware of all indices. Due to initializing shard throttling
        // not all shards are allocated with the initial create index. Wait for it..
        ensureYellow();

        final Throwable[] threadException = new Throwable[1];
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] threads = new Thread[3];
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final ArrayList<Client> clientArray = new ArrayList<Client>();
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
                            Map<String, MappingMetaData> mappings = getMappingResponse.getMappings().get(indexName);
                            assertThat(mappings.keySet(), Matchers.hasItem(typeName));
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
}
