/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

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
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), SCRIPT_MAX_SIZE_IN_BYTES)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomScriptPlugin.class);
    }

    public void testBasics() {
        assertAcked(clusterAdmin().preparePutStoredScript().setId("foobar").setContent(new BytesArray(Strings.format("""
            {"script": {"lang": "%s", "source": "1"} }
            """, LANG)), XContentType.JSON));
        String script = clusterAdmin().prepareGetStoredScript("foobar").get().getSource().getSource();
        assertNotNull(script);
        assertEquals("1", script);

        assertAcked(clusterAdmin().prepareDeleteStoredScript("foobar"));
        StoredScriptSource source = clusterAdmin().prepareGetStoredScript("foobar").get().getSource();
        assertNull(source);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            clusterAdmin().preparePutStoredScript().setId("id#").setContent(new BytesArray(Strings.format("""
                {"script": {"lang": "%s", "source": "1"} }
                """, LANG)), XContentType.JSON)
        );
        assertEquals("Validation Failed: 1: id cannot contain '#' for stored script;", e.getMessage());
    }

    public void testMaxScriptSize() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            clusterAdmin().preparePutStoredScript().setId("foobar").setContent(new BytesArray(Strings.format("""
                {"script": { "lang": "%s", "source":"0123456789abcdef"} }\
                """, LANG)), XContentType.JSON)
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
