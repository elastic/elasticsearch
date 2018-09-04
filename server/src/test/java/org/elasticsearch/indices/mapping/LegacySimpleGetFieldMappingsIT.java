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
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.indices.mapping.SimpleGetFieldMappingsIT.getMappingForType;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class LegacySimpleGetFieldMappingsIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testGetFieldMappingsMultiType() throws Exception {
        assertTrue("remove this multi type test", Version.CURRENT.before(Version.fromString("7.0.0")));
        assertAcked(prepareCreate("indexa")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("typeA", getMappingForType("typeA"))
                .addMapping("typeB", getMappingForType("typeB")));
        assertAcked(client().admin().indices().prepareCreate("indexb")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("typeA", getMappingForType("typeA"))
                .addMapping("typeB", getMappingForType("typeB")));


        // Get mappings by full name
        GetFieldMappingsResponse response =
                client().admin().indices().prepareGetFieldMappings("indexa").setTypes("typeA").setFields("field1", "obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "field1").fullName(), equalTo("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1").sourceAsMap(), hasKey("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.mappings().get("indexa"), not(hasKey("typeB")));
        assertThat(response.fieldMappings("indexa", "typeB", "field1"), nullValue());
        assertThat(response.mappings(), not(hasKey("indexb")));
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

        // Get mappings by name
        response = client().admin().indices().prepareGetFieldMappings("indexa").setTypes("typeA").setFields("field1", "obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "field1").fullName(), equalTo("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1").sourceAsMap(), hasKey("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

        // get mappings by name across multiple indices
        response = client().admin().indices().prepareGetFieldMappings().setTypes("typeA").setFields("obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "obj.subfield"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexb", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "obj.subfield"), nullValue());

        // get mappings by name across multiple types
        response = client().admin().indices().prepareGetFieldMappings("indexa").setFields("obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1"), nullValue());
        assertThat(response.fieldMappings("indexa", "typeB", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeA", "obj.subfield"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeA", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "obj.subfield"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

        // get mappings by name across multiple types & indices
        response = client().admin().indices().prepareGetFieldMappings().setFields("obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1"), nullValue());
        assertThat(response.fieldMappings("indexa", "typeB", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexb", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());
    }

}
