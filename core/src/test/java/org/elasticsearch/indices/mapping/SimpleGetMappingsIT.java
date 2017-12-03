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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_METADATA_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SimpleGetMappingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testGetMappingsWhereThereAreNone() {
        createIndex("index");
        GetMappingsResponse response = client().admin().indices().prepareGetMappings().execute().actionGet();
        assertThat(response.mappings().containsKey("index"), equalTo(true));
        assertThat(response.mappings().get("index").size(), equalTo(0));
    }

    private XContentBuilder getMappingForType(String type) throws IOException {
        return jsonBuilder().startObject().startObject(type).startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject().endObject().endObject();
    }

    public void testSimpleGetMappings() throws Exception {
        client().admin().indices().prepareCreate("indexa")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("typeA", getMappingForType("typeA"))
                .addMapping("typeB", getMappingForType("typeB"))
                .addMapping("Atype", getMappingForType("Atype"))
                .addMapping("Btype", getMappingForType("Btype"))
                .execute().actionGet();
        client().admin().indices().prepareCreate("indexb")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("typeA", getMappingForType("typeA"))
                .addMapping("typeB", getMappingForType("typeB"))
                .addMapping("Atype", getMappingForType("Atype"))
                .addMapping("Btype", getMappingForType("Btype"))
                .execute().actionGet();

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // Get all mappings
        GetMappingsResponse response = client().admin().indices().prepareGetMappings().execute().actionGet();
        assertThat(response.mappings().size(), equalTo(2));
        assertThat(response.mappings().get("indexa").size(), equalTo(4));
        assertThat(response.mappings().get("indexa").get("typeA"), notNullValue());
        assertThat(response.mappings().get("indexa").get("typeB"), notNullValue());
        assertThat(response.mappings().get("indexa").get("Atype"), notNullValue());
        assertThat(response.mappings().get("indexa").get("Btype"), notNullValue());
        assertThat(response.mappings().get("indexb").size(), equalTo(4));
        assertThat(response.mappings().get("indexb").get("typeA"), notNullValue());
        assertThat(response.mappings().get("indexb").get("typeB"), notNullValue());
        assertThat(response.mappings().get("indexb").get("Atype"), notNullValue());
        assertThat(response.mappings().get("indexb").get("Btype"), notNullValue());

        // Get all mappings, via wildcard support
        response = client().admin().indices().prepareGetMappings("*").setTypes("*").execute().actionGet();
        assertThat(response.mappings().size(), equalTo(2));
        assertThat(response.mappings().get("indexa").size(), equalTo(4));
        assertThat(response.mappings().get("indexa").get("typeA"), notNullValue());
        assertThat(response.mappings().get("indexa").get("typeB"), notNullValue());
        assertThat(response.mappings().get("indexa").get("Atype"), notNullValue());
        assertThat(response.mappings().get("indexa").get("Btype"), notNullValue());
        assertThat(response.mappings().get("indexb").size(), equalTo(4));
        assertThat(response.mappings().get("indexb").get("typeA"), notNullValue());
        assertThat(response.mappings().get("indexb").get("typeB"), notNullValue());
        assertThat(response.mappings().get("indexb").get("Atype"), notNullValue());
        assertThat(response.mappings().get("indexb").get("Btype"), notNullValue());

        // Get all typeA mappings in all indices
        response = client().admin().indices().prepareGetMappings("*").setTypes("typeA").execute().actionGet();
        assertThat(response.mappings().size(), equalTo(2));
        assertThat(response.mappings().get("indexa").size(), equalTo(1));
        assertThat(response.mappings().get("indexa").get("typeA"), notNullValue());
        assertThat(response.mappings().get("indexb").size(), equalTo(1));
        assertThat(response.mappings().get("indexb").get("typeA"), notNullValue());

        // Get all mappings in indexa
        response = client().admin().indices().prepareGetMappings("indexa").execute().actionGet();
        assertThat(response.mappings().size(), equalTo(1));
        assertThat(response.mappings().get("indexa").size(), equalTo(4));
        assertThat(response.mappings().get("indexa").get("typeA"), notNullValue());
        assertThat(response.mappings().get("indexa").get("typeB"), notNullValue());
        assertThat(response.mappings().get("indexa").get("Atype"), notNullValue());
        assertThat(response.mappings().get("indexa").get("Btype"), notNullValue());

        // Get all mappings beginning with A* in indexa
        response = client().admin().indices().prepareGetMappings("indexa").setTypes("A*").execute().actionGet();
        assertThat(response.mappings().size(), equalTo(1));
        assertThat(response.mappings().get("indexa").size(), equalTo(1));
        assertThat(response.mappings().get("indexa").get("Atype"), notNullValue());

        // Get all mappings beginning with B* in all indices
        response = client().admin().indices().prepareGetMappings().setTypes("B*").execute().actionGet();
        assertThat(response.mappings().size(), equalTo(2));
        assertThat(response.mappings().get("indexa").size(), equalTo(1));
        assertThat(response.mappings().get("indexa").get("Btype"), notNullValue());
        assertThat(response.mappings().get("indexb").size(), equalTo(1));
        assertThat(response.mappings().get("indexb").get("Btype"), notNullValue());

        // Get all mappings beginning with B* and A* in all indices
        response = client().admin().indices().prepareGetMappings().setTypes("B*", "A*").execute().actionGet();
        assertThat(response.mappings().size(), equalTo(2));
        assertThat(response.mappings().get("indexa").size(), equalTo(2));
        assertThat(response.mappings().get("indexa").get("Atype"), notNullValue());
        assertThat(response.mappings().get("indexa").get("Btype"), notNullValue());
        assertThat(response.mappings().get("indexb").size(), equalTo(2));
        assertThat(response.mappings().get("indexb").get("Atype"), notNullValue());
        assertThat(response.mappings().get("indexb").get("Btype"), notNullValue());
    }

    public void testGetMappingsWithBlocks() throws IOException {
        client().admin().indices().prepareCreate("test")
                .addMapping("doc", getMappingForType("doc"))
                .execute().actionGet();
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("test", block);
                GetMappingsResponse response = client().admin().indices().prepareGetMappings().execute().actionGet();
                assertThat(response.mappings().size(), equalTo(1));
                assertThat(response.mappings().get("test").size(), equalTo(1));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareGetMappings(), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }
}
