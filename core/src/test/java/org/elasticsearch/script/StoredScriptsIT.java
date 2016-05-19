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
package org.elasticsearch.script;

import org.elasticsearch.action.admin.cluster.validate.template.RenderSearchTemplateResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class StoredScriptsIT extends ESIntegTestCase {

    private final static int SCRIPT_MAX_SIZE_IN_BYTES = 64;
    private final static String LANG = MockScriptEngine.NAME;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), SCRIPT_MAX_SIZE_IN_BYTES)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockScriptEngine.TestPlugin.class);
    }

    public void testBasics() {
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setScriptLang(LANG)
                .setId("foobar")
                .setSource(new BytesArray("{\"script\":\"1\"}")));
        String script = client().admin().cluster().prepareGetStoredScript(LANG, "foobar")
                .get().getStoredScript();
        assertNotNull(script);
        assertEquals("1", script);

        RenderSearchTemplateResponse response = client().admin().cluster().prepareRenderSearchTemplate()
                .template(new Template("/" + LANG + "/foobar", ScriptService.ScriptType.STORED, LANG, null, null))
                .get();
        assertEquals("1", response.source().toUtf8());

        assertAcked(client().admin().cluster().prepareDeleteStoredScript()
                .setId("foobar")
                .setScriptLang(LANG));
        script = client().admin().cluster().prepareGetStoredScript(LANG, "foobar")
                .get().getStoredScript();
        assertNull(script);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin().cluster().preparePutStoredScript()
                .setScriptLang("lang#")
                .setId("id#")
                .setSource(new BytesArray("{}"))
                .get());
        assertEquals("Validation Failed: 1: id can't contain: '#';2: lang can't contain: '#';", e.getMessage());
    }

    public void testMaxScriptSize() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin().cluster().preparePutStoredScript()
                .setScriptLang(LANG)
                .setId("foobar")
                .setSource(new BytesArray(randomAsciiOfLength(SCRIPT_MAX_SIZE_IN_BYTES + 1)))
                .get()
        );
        assertEquals("Limit of script size in bytes [64] has been exceeded for script [foobar] with size [65]", e.getMessage());
    }

}
