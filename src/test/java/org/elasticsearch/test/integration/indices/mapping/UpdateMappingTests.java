package org.elasticsearch.test.integration.indices.mapping;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.*;

public class UpdateMappingTests extends AbstractSharedClusterTest {

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
        createIndexMapped("test", "type", "normal", "long", "exclude", "long", "include", "long");

        logger.info("Index doc 1");
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

        logger.info("Index doc 2");
        index("test", "type", "2", JsonXContent.contentBuilder().startObject()
                .field("normal", 2).field("exclude", 1).field("include", 2)
                .endObject()
        );

        GetResponse getResponse = get("test", "type", "2");
        assertThat(getResponse.getSource(), hasKey("normal"));
        assertThat(getResponse.getSource(), not(hasKey("exclude")));
        assertThat(getResponse.getSource(), hasKey("include"));


        putResponse = client().admin().indices().preparePutMapping("test").setType("type").setSource(
                JsonXContent.contentBuilder().startObject().startObject("type")
                        .startObject("_source")
                        .startArray("excludes").endArray()
                        .startArray("includes").value("include").endArray()
                        .endObject().endObject()
        ).get();
        assertTrue(putResponse.isAcknowledged());

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        MappingMetaData typeMapping = clusterStateResponse.getState().metaData().index("test").getMappings().get("type");
        assertThat((Map<String, Object>) typeMapping.getSourceAsMap().get("_source"), hasKey("includes"));
        assertThat((Map<String, Object>) typeMapping.getSourceAsMap().get("_source"), not(hasKey("excludes")));


        index("test", "type", "3", JsonXContent.contentBuilder().startObject()
                .field("normal", 3).field("exclude", 3).field("include", 3)
                .endObject()
        );

        getResponse = get("test", "type", "3");
        assertThat(getResponse.getSource(), not(hasKey("normal")));
        assertThat(getResponse.getSource(), not(hasKey("exclude")));
        assertThat(getResponse.getSource(), hasKey("include"));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void updateDefaultMappingSettings() throws Exception {

        // TODO: bleskes: move back to combined index and mapping creation (pending bug fix concerning concurrent not-acked mapping requests)
        createIndex("test");

        logger.info("Creating _default_ mappings");
        PutMappingResponse putResponse = client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .field("date_detection", false)
                        .endObject().endObject()
        ).get();
        assertThat(putResponse.isAcknowledged(), equalTo(true));
        logger.info("DONE: Creating _default_ mappings");

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        Map<String, Object> defaultMapping = clusterStateResponse.getState().metaData().index("test").getMappings().get(MapperService.DEFAULT_MAPPING).sourceAsMap();
        assertThat(defaultMapping, hasKey("date_detection"));


        logger.info("Emptying _default_ mappings");
        // now remove it
        putResponse = client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .endObject().endObject()
        ).get();
        assertThat(putResponse.isAcknowledged(), equalTo(true));
        logger.info("Done emptying _default_ mappings");

        clusterStateResponse = client().admin().cluster().prepareState().get();
        defaultMapping = clusterStateResponse.getState().metaData().index("test").getMappings().get(MapperService.DEFAULT_MAPPING).sourceAsMap();
        assertThat(defaultMapping, not(hasKey("date_detection")));

        // now test you can change stuff that are normally unchangable
        logger.info("Creating _default_ mappings with an analyzed field");
        putResponse = client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .startObject("properties").startObject("f").field("type", "string").field("index", "analyzed").endObject().endObject()
                        .endObject().endObject()
        ).get();
        assertThat(putResponse.isAcknowledged(), equalTo(true));
        logger.info("Done creating _default_ mappings with an analyzed field");


        logger.info("Changing _default_ mappings field from analyzed to non-analyzed");
        putResponse = client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(
                JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                        .startObject("properties").startObject("f").field("type", "string").field("index", "not_analyzed").endObject().endObject()
                        .endObject().endObject()
        ).get();
        assertThat(putResponse.isAcknowledged(), equalTo(true));
        logger.info("Done changing _default_ mappings field from analyzed to non-analyzed");

        clusterStateResponse = client().admin().cluster().prepareState().get();
        defaultMapping = clusterStateResponse.getState().metaData().index("test").getMappings().get(MapperService.DEFAULT_MAPPING).sourceAsMap();

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
}
