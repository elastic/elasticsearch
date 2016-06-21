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
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class UpdateMappingOnClusterIT extends ESIntegTestCase {
    private static final String INDEX = "index";
    private static final String TYPE = "type";

    public void testAllEnabled() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("mappings").startObject(TYPE).startObject("_all").field("enabled", "false").endObject().endObject().endObject().endObject();
        XContentBuilder mappingUpdate = jsonBuilder().startObject().startObject("_all").field("enabled", "true").endObject().startObject("properties").startObject("text").field("type", "text").endObject().endObject().endObject();
        String errorMessage = "[_all] enabled is false now encountering true";
        testConflict(mapping.string(), mappingUpdate.string(), errorMessage);
    }

    public void testAllConflicts() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/update/all_mapping_create_index.json");
        String mappingUpdate = copyToStringFromClasspath("/org/elasticsearch/index/mapper/update/all_mapping_update_with_conflicts.json");
        String[] errorMessage = {
                "[_all] has different [norms] values",
                "[_all] has different [store] values",
                "[_all] has different [store_term_vector] values",
                "[_all] has different [store_term_vector_offsets] values",
                "[_all] has different [store_term_vector_positions] values",
                "[_all] has different [store_term_vector_payloads] values",
                "[_all] has different [analyzer]",
                "[_all] has different [similarity]"};
        // fielddata and search_analyzer should not report conflict
        testConflict(mapping, mappingUpdate, errorMessage);
    }

    public void testAllDisabled() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("mappings").startObject(TYPE).startObject("_all").field("enabled", true).endObject().endObject().endObject().endObject();
        XContentBuilder mappingUpdate = jsonBuilder().startObject().startObject("_all").field("enabled", false).endObject().startObject("properties").startObject("text").field("type", "text").endObject().endObject().endObject();
        String errorMessage = "[_all] enabled is true now encountering false";
        testConflict(mapping.string(), mappingUpdate.string(), errorMessage);
    }

    public void testAllWithDefault() throws Exception {
        String defaultMapping = jsonBuilder().startObject().startObject("_default_")
                .startObject("_all")
                .field("enabled", false)
                .endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("index").addMapping("_default_", defaultMapping).get();
        String docMapping = jsonBuilder().startObject()
                .startObject("doc")
                .endObject()
                .endObject().string();
        PutMappingResponse response = client().admin().indices().preparePutMapping("index").setType("doc").setSource(docMapping).get();
        assertTrue(response.isAcknowledged());
        String docMappingUpdate = jsonBuilder().startObject().startObject("doc")
                .startObject("properties")
                .startObject("text")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject().string();
        response = client().admin().indices().preparePutMapping("index").setType("doc").setSource(docMappingUpdate).get();
        assertTrue(response.isAcknowledged());
        String docMappingAllExplicitEnabled = jsonBuilder().startObject()
                .startObject("doc_all_enabled")
                .startObject("_all")
                .field("enabled", true)
                .endObject()
                .endObject()
                .endObject().string();
        response = client().admin().indices().preparePutMapping("index").setType("doc_all_enabled").setSource(docMappingAllExplicitEnabled).get();
        assertTrue(response.isAcknowledged());

        GetMappingsResponse mapping = client().admin().indices().prepareGetMappings("index").get();
        HashMap props = (HashMap)mapping.getMappings().get("index").get("doc").getSourceAsMap().get("_all");
        assertThat((Boolean)props.get("enabled"), equalTo(false));
        props = (HashMap)mapping.getMappings().get("index").get("doc").getSourceAsMap().get("properties");
        assertNotNull(props);
        assertNotNull(props.get("text"));
        props = (HashMap)mapping.getMappings().get("index").get("doc_all_enabled").getSourceAsMap().get("_all");
        assertThat((Boolean)props.get("enabled"), equalTo(true));
        props = (HashMap)mapping.getMappings().get("index").get("_default_").getSourceAsMap().get("_all");
        assertThat((Boolean)props.get("enabled"), equalTo(false));

    }

    public void testDocValuesInvalidMapping() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("mappings").startObject(TYPE).startObject("_all").startObject("fielddata").field("format", "doc_values").endObject().endObject().endObject().endObject().endObject().string();
        try {
            prepareCreate(INDEX).setSource(mapping).get();
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("[_all] is always tokenized and cannot have doc values"));
        }
    }

    public void testDocValuesInvalidMappingOnUpdate() throws Exception {
        String mapping = jsonBuilder().startObject().startObject(TYPE).startObject("properties").startObject("text").field("type", "text").endObject().endObject().endObject().endObject().string();
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
        } catch (IllegalArgumentException e) {
            for (String errorMessage : errorMessages) {
                assertThat(e.getMessage(), containsString(errorMessage));
            }
        }
        compareMappingOnNodes(mappingsBeforeUpdateResponse);

    }

    private void compareMappingOnNodes(GetMappingsResponse previousMapping) {
        // make sure all nodes have same cluster state
        for (Client client : cluster().getClients()) {
            GetMappingsResponse currentMapping = client.admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).setLocal(true).get();
            assertThat(previousMapping.getMappings().get(INDEX).get(TYPE).source(), equalTo(currentMapping.getMappings().get(INDEX).get(TYPE).source()));
        }
    }
}
