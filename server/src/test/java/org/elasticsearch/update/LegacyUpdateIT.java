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

package org.elasticsearch.update;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class LegacyUpdateIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(UpdateIT.UpdateScriptsPlugin.class);
    }

    private static final String UPDATE_SCRIPTS = "update_scripts";
    private static final String EXTRACT_CTX_SCRIPT = "extract_ctx";

    public void testContextVariables() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addAlias(new Alias("alias"))
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .endObject()
                        .endObject())
                .addMapping("subtype1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("subtype1")
                        .startObject("_parent").field("type", "type1").endObject()
                        .endObject()
                        .endObject())
        );
        ensureGreen();

        // Index some documents
        client().prepareIndex()
                .setIndex("test")
                .setType("type1")
                .setId("parentId1")
                .setSource("field1", 0, "content", "bar")
                .execute().actionGet();

        client().prepareIndex()
                .setIndex("test")
                .setType("subtype1")
                .setId("id1")
                .setParent("parentId1")
                .setRouting("routing1")
                .setSource("field1", 1, "content", "foo")
                .execute().actionGet();

        // Update the first object and note context variables values
        UpdateResponse updateResponse = client().prepareUpdate("test", "subtype1", "id1")
                .setRouting("routing1")
                .setScript(new Script(ScriptType.INLINE, UPDATE_SCRIPTS, EXTRACT_CTX_SCRIPT, Collections.emptyMap()))
                .execute().actionGet();

        assertEquals(2, updateResponse.getVersion());

        GetResponse getResponse = client().prepareGet("test", "subtype1", "id1").setRouting("routing1").execute().actionGet();
        Map<String, Object> updateContext = (Map<String, Object>) getResponse.getSourceAsMap().get("update_context");
        assertEquals("test", updateContext.get("_index"));
        assertEquals("subtype1", updateContext.get("_type"));
        assertEquals("id1", updateContext.get("_id"));
        assertEquals(1, updateContext.get("_version"));
        assertEquals("parentId1", updateContext.get("_parent"));
        assertEquals("routing1", updateContext.get("_routing"));

        // Idem with the second object
        updateResponse = client().prepareUpdate("test", "type1", "parentId1")
                .setScript(new Script(ScriptType.INLINE, UPDATE_SCRIPTS, EXTRACT_CTX_SCRIPT, Collections.emptyMap()))
                .execute().actionGet();

        assertEquals(2, updateResponse.getVersion());

        getResponse = client().prepareGet("test", "type1", "parentId1").execute().actionGet();
        updateContext = (Map<String, Object>) getResponse.getSourceAsMap().get("update_context");
        assertEquals("test", updateContext.get("_index"));
        assertEquals("type1", updateContext.get("_type"));
        assertEquals("parentId1", updateContext.get("_id"));
        assertEquals(1, updateContext.get("_version"));
        assertNull(updateContext.get("_parent"));
        assertNull(updateContext.get("_routing"));
        assertNull(updateContext.get("_ttl"));
    }

}
