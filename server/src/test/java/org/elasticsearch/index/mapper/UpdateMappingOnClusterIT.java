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

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class UpdateMappingOnClusterIT extends ESIntegTestCase {
    private static final String INDEX = "index";
    private static final String TYPE = "type";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class); // uses index.version.created
    }

    protected void testConflict(String mapping, String mappingUpdate, Version idxVersion, String... errorMessages) throws InterruptedException {
        assertAcked(prepareCreate(INDEX).setSource(mapping, XContentType.JSON)
            .setSettings(Settings.builder().put("index.version.created", idxVersion.id)));
        ensureGreen(INDEX);
        GetMappingsResponse mappingsBeforeUpdateResponse = client().admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).get();
        try {
            client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(mappingUpdate, XContentType.JSON).get();
            fail();
        } catch (IllegalArgumentException e) {
            for (String errorMessage : errorMessages) {
                assertThat(e.getMessage(), containsString(errorMessage));
            }
        }
        compareMappingOnNodes(mappingsBeforeUpdateResponse);

    }

    public void testUpdatingAllSettingsOnOlderIndex() throws Exception {
        XContentBuilder mapping = jsonBuilder()
                .startObject()
                .startObject("mappings")
                .startObject(TYPE)
                .startObject("_all").field("enabled", "true").endObject()
                .endObject()
                .endObject()
                .endObject();
        XContentBuilder mappingUpdate = jsonBuilder()
                .startObject()
                .startObject("_all").field("enabled", "false").endObject()
                .startObject("properties").startObject("text").field("type", "text").endObject()
                .endObject()
                .endObject();
        String errorMessage = "[_all] enabled is true now encountering false";
        testConflict(mapping.string(), mappingUpdate.string(), Version.V_5_0_0, errorMessage);
    }

    public void testUpdatingAllSettingsOnOlderIndexDisabledToEnabled() throws Exception {
        XContentBuilder mapping = jsonBuilder()
                .startObject()
                .startObject("mappings")
                .startObject(TYPE)
                .startObject("_all").field("enabled", "false").endObject()
                .endObject()
                .endObject()
                .endObject();
        XContentBuilder mappingUpdate = jsonBuilder()
                .startObject()
                .startObject("_all").field("enabled", "true").endObject()
                .startObject("properties").startObject("text").field("type", "text").endObject()
                .endObject()
                .endObject();
        String errorMessage = "[_all] enabled is false now encountering true";
        testConflict(mapping.string(), mappingUpdate.string(), Version.V_5_0_0, errorMessage);
    }

    private void compareMappingOnNodes(GetMappingsResponse previousMapping) {
        // make sure all nodes have same cluster state
        for (Client client : cluster().getClients()) {
            GetMappingsResponse currentMapping = client.admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).setLocal(true).get();
            assertThat(previousMapping.getMappings().get(INDEX).get(TYPE).source(), equalTo(currentMapping.getMappings().get(INDEX).get(TYPE).source()));
        }
    }
}
