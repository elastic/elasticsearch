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

package org.elasticsearch.index.mapper.update;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;


public class UpdateMappingOnClusterTests extends ElasticsearchIntegrationTest {

    private static final String INDEX = "index";
    private static final String TYPE = "type";


    @Test
    public void test_all_enabled() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("mappings").startObject(TYPE).startObject("_all").field("enabled", "false").endObject().endObject().endObject().endObject();
        XContentBuilder mappingUpdate = jsonBuilder().startObject().startObject("_all").field("enabled", "true").endObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject();
        String errorMessage = "[_all] enabled is false now encountering true";
        testConflict(mapping.string(), mappingUpdate.string(), errorMessage);
    }

    @Test
    public void test_all_conflicts() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/update/all_mapping_create_index.json");
        String mappingUpdate = copyToStringFromClasspath("/org/elasticsearch/index/mapper/update/all_mapping_update_with_conflicts.json");
        String[] errorMessage = {"[_all] enabled is true now encountering false",
                "[_all] cannot enable norms (`norms.enabled`)",
                "[_all] has different store values",
                "[_all] has different store_term_vector values",
                "[_all] has different store_term_vector_offsets values",
                "[_all] has different store_term_vector_positions values",
                "[_all] has different store_term_vector_payloads values",
                "[_all] has different index_analyzer",
                "[_all] has different similarity"};
        // auto_boost and fielddata and search_analyzer should not report conflict
        testConflict(mapping, mappingUpdate, errorMessage);
    }

    @Test
    public void test_doc_valuesInvalidMapping() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("mappings").startObject(TYPE).startObject("_all").startObject("fielddata").field("format", "doc_values").endObject().endObject().endObject().endObject().endObject().string();
        try {
            prepareCreate(INDEX).setSource(mapping).get();
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("[_all] is always tokenized and cannot have doc values"));
        }
    }

    @Test
    public void test_doc_valuesInvalidMappingOnUpdate() throws Exception {
        String mapping = jsonBuilder().startObject().startObject(TYPE).startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject().string();
        prepareCreate(INDEX).addMapping(TYPE, mapping).get();
        String mappingUpdate = jsonBuilder().startObject().startObject(TYPE).startObject("_all").startObject("fielddata").field("format", "doc_values").endObject().endObject().endObject().endObject().string();
        GetMappingsResponse mappingsBeforeUpdateResponse = client().admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).get();
        try {
            client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(mappingUpdate).get();
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("[_all] is always tokenized and cannot have doc values"));
        }
        // make sure all nodes have same cluster state
        compareMappingOnNodes(mappingsBeforeUpdateResponse);
    }

    // checks if the setting for timestamp and size are kept even if disabled
    @Test
    public void testDisabledSizeTimestampIndexDoNotLooseMappings() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/update/default_mapping_with_disabled_root_types.json");
        prepareCreate(INDEX).addMapping(TYPE, mapping).get();
        GetMappingsResponse mappingsBeforeGreen = client().admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).get();
        ensureGreen(INDEX);
        // make sure all nodes have same cluster state
        compareMappingOnNodes(mappingsBeforeGreen);
    }

    protected void testConflict(String mapping, String mappingUpdate, String... errorMessages) throws InterruptedException {
        assertAcked(prepareCreate(INDEX).setSource(mapping).get());
        ensureGreen(INDEX);
        GetMappingsResponse mappingsBeforeUpdateResponse = client().admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).get();
        try {
            client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(mappingUpdate).get();
            fail();
        } catch (MergeMappingException e) {
            for (String errorMessage : errorMessages) {
                assertThat(e.getDetailedMessage(), containsString(errorMessage));
            }
        }
        compareMappingOnNodes(mappingsBeforeUpdateResponse);

    }

    private void compareMappingOnNodes(GetMappingsResponse previousMapping) {
        // make sure all nodes have same cluster state
        for (Client client : cluster()) {
            GetMappingsResponse currentMapping = client.admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).setLocal(true).get();
            assertThat(previousMapping.getMappings().get(INDEX).get(TYPE).source(), equalTo(currentMapping.getMappings().get(INDEX).get(TYPE).source()));
        }
    }

    @Test
    public void testUpdateTimestamp() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", randomBoolean()).startObject("fielddata").field("loading", "lazy").field("format", "doc_values").endObject().field("store", "yes").endObject()
                .endObject().endObject();
        client().admin().indices().prepareCreate("test").addMapping("type", mapping).get();
        GetMappingsResponse appliedMappings = client().admin().indices().prepareGetMappings("test").get();
        LinkedHashMap timestampMapping = (LinkedHashMap) appliedMappings.getMappings().get("test").get("type").getSourceAsMap().get("_timestamp");
        assertThat((Boolean) timestampMapping.get("store"), equalTo(true));
        assertThat((String)((LinkedHashMap) timestampMapping.get("fielddata")).get("loading"), equalTo("lazy"));
        assertThat((String)((LinkedHashMap) timestampMapping.get("fielddata")).get("format"), equalTo("doc_values"));
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", randomBoolean()).startObject("fielddata").field("loading", "eager").field("format", "array").endObject().field("store", "yes").endObject()
                .endObject().endObject();
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();
        appliedMappings = client().admin().indices().prepareGetMappings("test").get();
        timestampMapping = (LinkedHashMap) appliedMappings.getMappings().get("test").get("type").getSourceAsMap().get("_timestamp");
        assertThat((Boolean) timestampMapping.get("store"), equalTo(true));
        assertThat((String)((LinkedHashMap) timestampMapping.get("fielddata")).get("loading"), equalTo("eager"));
        assertThat((String)((LinkedHashMap) timestampMapping.get("fielddata")).get("format"), equalTo("array"));
    }

    @Test
    public void testTimestampMergingConflicts() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject(TYPE)
                .startObject("_timestamp").field("enabled", true)
                .startObject("fielddata").field("format", "doc_values").endObject()
                .field("store", "yes")
                .field("index", "analyzed")
                .field("path", "foo")
                .field("default", "1970-01-01")
                .endObject()
                .endObject().endObject().string();

        client().admin().indices().prepareCreate(INDEX).addMapping(TYPE, mapping).get();

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", false)
                .startObject("fielddata").field("format", "array").endObject()
                .field("store", "no")
                .field("index", "no")
                .field("path", "bar")
                .field("default", "1970-01-02")
                .endObject()
                .endObject().endObject().string();
        GetMappingsResponse mappingsBeforeUpdateResponse = client().admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).get();
        try {
            client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(mapping).get();
            fail("This should result in conflicts when merging the mapping");
        } catch (MergeMappingException e) {
            String[] expectedConflicts = {"mapper [_timestamp] has different index values", "mapper [_timestamp] has different store values", "Cannot update default in _timestamp value. Value is 1970-01-01 now encountering 1970-01-02", "Cannot update path in _timestamp value. Value is foo path in merged mapping is bar"};
            for (String conflict : expectedConflicts) {
                assertThat(e.getDetailedMessage(), containsString(conflict));
            }
        }
        compareMappingOnNodes(mappingsBeforeUpdateResponse);
    }
}
