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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class LegacyUpdateMappingIntegrationIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDefaultMappingSettings() throws Exception {
        logger.info("Creating index with _default_ mappings");
        try (XContentBuilder defaultMapping = JsonXContent.contentBuilder()) {
            defaultMapping.startObject();
            {
                defaultMapping.startObject(MapperService.DEFAULT_MAPPING);
                {
                    defaultMapping.field("date_detection", false);
                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();
            client()
                    .admin()
                    .indices()
                    .prepareCreate("test")
                    .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_3_0).build())
                    .addMapping(MapperService.DEFAULT_MAPPING, defaultMapping)
                    .get();
        }

        {
            final GetMappingsResponse getResponse =
                    client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
            final Map<String, Object> defaultMapping =
                    getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
            assertThat(defaultMapping, hasKey("date_detection"));
        }

        logger.info("Emptying _default_ mappings");
        // now remove it
        try (XContentBuilder mappingBuilder =
                     JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING).endObject().endObject()) {
            final AcknowledgedResponse putResponse =
                    client()
                            .admin()
                            .indices()
                            .preparePutMapping("test")
                            .setType(MapperService.DEFAULT_MAPPING)
                            .setSource(mappingBuilder)
                            .get();
            assertThat(putResponse.isAcknowledged(), equalTo(true));
        }
        logger.info("Done Emptying _default_ mappings");

        {
            final GetMappingsResponse getResponse =
                    client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
            final Map<String, Object> defaultMapping =
                    getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
            assertThat(defaultMapping, not(hasKey("date_detection")));
        }

        // now test you can change stuff that are normally unchangeable
        logger.info("Creating _default_ mappings with an analyzed field");
        try (XContentBuilder defaultMapping = JsonXContent.contentBuilder()) {

            defaultMapping.startObject();
            {
                defaultMapping.startObject(MapperService.DEFAULT_MAPPING);
                {
                    defaultMapping.startObject("properties");
                    {
                        defaultMapping.startObject("f");
                        {
                            defaultMapping.field("type", "text");
                            defaultMapping.field("index", true);
                        }
                        defaultMapping.endObject();
                    }
                    defaultMapping.endObject();
                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();

            final AcknowledgedResponse putResponse =
                    client()
                            .admin()
                            .indices()
                            .preparePutMapping("test")
                            .setType(MapperService.DEFAULT_MAPPING).setSource(defaultMapping)
                            .get();
            assertThat(putResponse.isAcknowledged(), equalTo(true));
        }

        logger.info("Changing _default_ mappings field from analyzed to non-analyzed");
        {
            try (XContentBuilder mappingBuilder = JsonXContent.contentBuilder()) {
                mappingBuilder.startObject();
                {
                    mappingBuilder.startObject(MapperService.DEFAULT_MAPPING);
                    {
                        mappingBuilder.startObject("properties");
                        {
                            mappingBuilder.startObject("f");
                            {
                                mappingBuilder.field("type", "keyword");
                            }
                            mappingBuilder.endObject();
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();

                final AcknowledgedResponse putResponse =
                        client()
                                .admin()
                                .indices()
                                .preparePutMapping("test")
                                .setType(MapperService.DEFAULT_MAPPING)
                                .setSource(mappingBuilder)
                                .get();
                assertThat(putResponse.isAcknowledged(), equalTo(true));
            }
        }
        logger.info("Done changing _default_ mappings field from analyzed to non-analyzed");

        {
            final GetMappingsResponse getResponse =
                    client().admin().indices().prepareGetMappings("test").addTypes(MapperService.DEFAULT_MAPPING).get();
            final Map<String, Object> defaultMapping =
                    getResponse.getMappings().get("test").get(MapperService.DEFAULT_MAPPING).sourceAsMap();
            final Map<String, Object> fieldSettings = (Map<String, Object>) ((Map) defaultMapping.get("properties")).get("f");
            assertThat(fieldSettings, hasEntry("type", "keyword"));
        }

        // but we still validate the _default_ type
        logger.info("Confirming _default_ mappings validation");
        try (XContentBuilder mappingBuilder = JsonXContent.contentBuilder()) {

            mappingBuilder.startObject();
            {
                mappingBuilder.startObject(MapperService.DEFAULT_MAPPING);
                {
                    mappingBuilder.startObject("properites");
                    {
                        mappingBuilder.startObject("f");
                        {
                            mappingBuilder.field("type", "non-existent");
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();

            expectThrows(
                    MapperParsingException.class,
                    () -> client()
                            .admin()
                            .indices()
                            .preparePutMapping("test")
                            .setType(MapperService.DEFAULT_MAPPING)
                            .setSource(mappingBuilder)
                            .get());
        }

    }

    /**
     * Asserts that the root cause of mapping conflicts is readable.
     */
    public void testMappingConflictRootCause() throws Exception {
        CreateIndexRequestBuilder b = prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(),
                        VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, Version.V_5_6_9)));
        b.addMapping("type1", jsonBuilder().startObject().startObject("properties")
                .startObject("text")
                .field("type", "text")
                .field("analyzer", "standard")
                .field("search_analyzer", "whitespace")
                .endObject().endObject().endObject());
        b.addMapping("type2", jsonBuilder().humanReadable(true).startObject().startObject("properties")
                .startObject("text")
                .field("type", "text")
                .endObject().endObject().endObject());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> b.get());
        assertThat(e.getMessage(), containsString("mapper [text] is used by multiple types"));
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

    @SuppressWarnings("unchecked")
    public void testUpdateMappingOnAllTypes() {
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
