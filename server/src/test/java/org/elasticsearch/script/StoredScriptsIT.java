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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class StoredScriptsIT extends ESIntegTestCase {

    private static final int SCRIPT_MAX_SIZE_IN_BYTES = 64;
    private static final String LANG = MockScriptEngine.NAME;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), SCRIPT_MAX_SIZE_IN_BYTES)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomScriptPlugin.class);
    }

    public void testBasics() {
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("foobar")
                .setContent(new BytesArray("{\"script\": {\"lang\": \"" + LANG + "\", \"source\": \"1\"} }"), XContentType.JSON));
        String script = client().admin().cluster().prepareGetStoredScript("foobar")
                .get().getSource().getSource();
        assertNotNull(script);
        assertEquals("1", script);

        assertAcked(client().admin().cluster().prepareDeleteStoredScript()
                .setId("foobar"));
        StoredScriptSource source = client().admin().cluster().prepareGetStoredScript("foobar")
                .get().getSource();
        assertNull(source);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin().cluster().preparePutStoredScript()
                .setId("id#")
                .setContent(new BytesArray("{}"), XContentType.JSON)
                .get());
        assertEquals("Validation Failed: 1: id cannot contain '#' for stored script;", e.getMessage());
    }

    public void testMaxScriptSize() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin().cluster().preparePutStoredScript()
                .setId("foobar")
                .setContent(new BytesArray("{\"script\": { \"lang\": \"" + LANG + "\"," +
                        " \"source\":\"0123456789abcdef\"} }"), XContentType.JSON)
                .get()
        );
        assertEquals("exceeded max allowed stored script size in bytes [64] with size [65] for script [foobar]", e.getMessage());
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("1", script -> "1");
        }
    }
}
