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
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.indices.mapping.SimpleGetMappingsIT.getMappingForType;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class LegacySimpleGetMappingsIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
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

        ClusterHealthResponse clusterHealth =
                client()
                        .admin()
                        .cluster()
                        .prepareHealth()
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForGreenStatus()
                        .execute()
                        .actionGet();
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

}
