/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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

import static org.elasticsearch.action.admin.cluster.storedscripts.StoredScriptIntegTestUtils.putJsonStoredScript;
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
        putJsonStoredScript("foobar", Strings.format("""
            {"script": {"lang": "%s", "source": "1"} }
            """, LANG));
        String script = safeExecute(GetStoredScriptAction.INSTANCE, new GetStoredScriptRequest(TEST_REQUEST_TIMEOUT, "foobar")).getSource()
            .getSource();
        assertNotNull(script);
        assertEquals("1", script);

        assertAcked(
            safeExecute(
                TransportDeleteStoredScriptAction.TYPE,
                new DeleteStoredScriptRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "foobar")
            )
        );
        StoredScriptSource source = safeExecute(GetStoredScriptAction.INSTANCE, new GetStoredScriptRequest(TEST_REQUEST_TIMEOUT, "foobar"))
            .getSource();
        assertNull(source);

        assertEquals(
            "Validation Failed: 1: id cannot contain '#' for stored script;",
            asInstanceOf(
                IllegalArgumentException.class,
                ExceptionsHelper.unwrapCause(
                    safeAwaitFailure(
                        AcknowledgedResponse.class,
                        l -> client().execute(
                            TransportPutStoredScriptAction.TYPE,
                            new PutStoredScriptRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).id("id#")
                                .content(new BytesArray(Strings.format("""
                                    {"script": {"lang": "%s", "source": "1"} }
                                    """, LANG)), XContentType.JSON),
                            l
                        )
                    )
                )
            ).getMessage()
        );
    }

    public void testMaxScriptSize() {
        assertEquals(
            "exceeded max allowed stored script size in bytes [64] with size [65] for script [foobar]",
            asInstanceOf(
                IllegalArgumentException.class,
                ExceptionsHelper.unwrapCause(
                    safeAwaitFailure(
                        AcknowledgedResponse.class,
                        l -> client().execute(
                            TransportPutStoredScriptAction.TYPE,
                            new PutStoredScriptRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).id("foobar")
                                .content(new BytesArray(Strings.format("""
                                    {"script": { "lang": "%s", "source":"0123456789abcdef"} }\
                                    """, LANG)), XContentType.JSON),
                            l
                        )

                    )
                )
            ).getMessage()
        );
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("1", script -> "1");
        }
    }
}
