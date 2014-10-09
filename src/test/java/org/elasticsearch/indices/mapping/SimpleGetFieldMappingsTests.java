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

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleGetFieldMappingsTests extends ElasticsearchIntegrationTest {

    @Test
    public void getMappingsWhereThereAreNone() {
        createIndex("index");
        ensureYellow();
        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings().get();
        assertThat(response.mappings().size(), equalTo(1));
        assertThat(response.mappings().get("index").size(), equalTo(0));

        assertThat(response.fieldMappings("index", "type", "field"), Matchers.nullValue());
    }

    private XContentBuilder getMappingForType(String type) throws IOException {
        return jsonBuilder().startObject().startObject(type).startObject("properties")
                .startObject("field1").field("type", "string").endObject()
                .startObject("obj").startObject("properties").startObject("subfield").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                .endObject().endObject().endObject();
    }

    @Test
    public void simpleGetFieldMappings() throws Exception {

        assertAcked(prepareCreate("indexa")
                .addMapping("typeA", getMappingForType("typeA"))
                .addMapping("typeB", getMappingForType("typeB")));
        assertAcked(client().admin().indices().prepareCreate("indexb")
                .addMapping("typeA", getMappingForType("typeA"))
                .addMapping("typeB", getMappingForType("typeB")));

        ensureYellow();

        // Get mappings by full name
        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings("indexa").setTypes("typeA").setFields("field1", "obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "field1").fullName(), equalTo("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1").sourceAsMap(), hasKey("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.mappings().get("indexa"), not(hasKey("typeB")));
        assertThat(response.fieldMappings("indexa", "typeB", "field1"), nullValue());
        assertThat(response.mappings(), not(hasKey("indexb")));
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

        // Get mappings by name
        response = client().admin().indices().prepareGetFieldMappings("indexa").setTypes("typeA").setFields("field1", "subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "field1").fullName(), equalTo("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1").sourceAsMap(), hasKey("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

        // get mappings by name across multiple indices
        response = client().admin().indices().prepareGetFieldMappings().setTypes("typeA").setFields("subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "subfield"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeA", "subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexb", "typeA", "subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "subfield"), nullValue());

        // get mappings by name across multiple types
        response = client().admin().indices().prepareGetFieldMappings("indexa").setFields("subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1"), nullValue());
        assertThat(response.fieldMappings("indexa", "typeB", "subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeA", "subfield"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeA", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "subfield"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

        // get mappings by name across multiple types & indices
        response = client().admin().indices().prepareGetFieldMappings().setFields("subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1"), nullValue());
        assertThat(response.fieldMappings("indexa", "typeB", "subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeA", "subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexb", "typeA", "subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

    }

    @SuppressWarnings("unchecked")
    @Test
    public void simpleGetFieldMappingsWithDefaults() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", getMappingForType("type")));

        client().prepareIndex("test", "type", "1").setSource("num", 1).get();
        ensureYellow();
        waitForConcreteMappingsOnAll("test", "type", "num"); // for num, we need to wait...

        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings().setFields("num", "field1", "subfield").includeDefaults(true).get();

        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "num").sourceAsMap().get("num"), hasEntry("index", (Object) "not_analyzed"));
        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "num").sourceAsMap().get("num"), hasEntry("type", (Object) "long"));
        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "field1").sourceAsMap().get("field1"), hasEntry("index", (Object) "analyzed"));
        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "field1").sourceAsMap().get("field1"), hasEntry("type", (Object) "string"));
        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "subfield").sourceAsMap().get("subfield"), hasEntry("index", (Object) "not_analyzed"));
        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "subfield").sourceAsMap().get("subfield"), hasEntry("type", (Object) "string"));


    }
}
