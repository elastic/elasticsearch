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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.ContextAndHeaderHolder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.expression.ExpressionScriptEngineService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;

public class CustomScriptContextIT extends ESIntegTestCase {

    private static final ImmutableSet<String> LANG_SET = ImmutableSet.of(GroovyScriptEngineService.NAME, MustacheScriptEngineService.NAME, ExpressionScriptEngineService.NAME);

    private static final String PLUGIN_NAME = "testplugin";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("script." + PLUGIN_NAME + "_custom_globally_disabled_op", "off")
                .put("script.engine.expression.inline." + PLUGIN_NAME + "_custom_exp_disabled_op", "off")
                        .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CustomScriptContextPlugin.class);
    }

    @Test
    public void testCustomScriptContextsSettings() {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();

        ScriptService scriptService = internalCluster().getInstance(ScriptService.class);
        for (String lang : LANG_SET) {
            for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
                try {
                    scriptService.compile(new Script("test", scriptType, lang, null), new ScriptContext.Plugin(PLUGIN_NAME,
                            "custom_globally_disabled_op"), contextAndHeaders);
                    fail("script compilation should have been rejected");
                } catch(ScriptException e) {
                    assertThat(e.getMessage(), containsString("scripts of type [" + scriptType + "], operation [" + PLUGIN_NAME + "_custom_globally_disabled_op] and lang [" + lang + "] are disabled"));
                }
            }
        }

        try {
            scriptService.compile(new Script("1", ScriptService.ScriptType.INLINE, "expression", null), new ScriptContext.Plugin(
                    PLUGIN_NAME, "custom_exp_disabled_op"), contextAndHeaders);
            fail("script compilation should have been rejected");
        } catch(ScriptException e) {
            assertThat(e.getMessage(), containsString("scripts of type [inline], operation [" + PLUGIN_NAME + "_custom_exp_disabled_op] and lang [expression] are disabled"));
        }

        CompiledScript compiledScript = scriptService.compile(new Script("1", ScriptService.ScriptType.INLINE, "expression", null),
                randomFrom(new ScriptContext[] { ScriptContext.Standard.AGGS, ScriptContext.Standard.SEARCH }), contextAndHeaders);
        assertThat(compiledScript, notNullValue());

        compiledScript = scriptService.compile(new Script("1", ScriptService.ScriptType.INLINE, "mustache", null),
                new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"), contextAndHeaders);
        assertThat(compiledScript, notNullValue());

        for (String lang : LANG_SET) {
            compiledScript = scriptService.compile(new Script("1", ScriptService.ScriptType.INLINE, lang, null), new ScriptContext.Plugin(
                    PLUGIN_NAME, "custom_op"), contextAndHeaders);
            assertThat(compiledScript, notNullValue());
        }
    }

    @Test
    public void testCompileNonRegisteredPluginContext() {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        ScriptService scriptService = internalCluster().getInstance(ScriptService.class);
        try {
            scriptService.compile(
                    new Script("test", randomFrom(ScriptService.ScriptType.values()), randomFrom(LANG_SET.toArray(new String[LANG_SET
                            .size()])), null), new ScriptContext.Plugin("test", "unknown"), contextAndHeaders);
            fail("script compilation should have been rejected");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("script context [test_unknown] not supported"));
        }
    }

    @Test
    public void testCompileNonRegisteredScriptContext() {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        ScriptService scriptService = internalCluster().getInstance(ScriptService.class);
        try {
            scriptService.compile(
                    new Script("test", randomFrom(ScriptService.ScriptType.values()), randomFrom(LANG_SET.toArray(new String[LANG_SET
                            .size()])), null), new ScriptContext() {
                @Override
                public String getKey() {
                    return "test";
                }
                    }, contextAndHeaders);
            fail("script compilation should have been rejected");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("script context [test] not supported"));
        }
    }

    public static class CustomScriptContextPlugin extends Plugin {
        @Override
        public String name() {
            return "custom_script_context_plugin";
        }

        @Override
        public String description() {
            return "Custom script context plugin";
        }

        public void onModule(ScriptModule scriptModule) {
                scriptModule.registerScriptContext(new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"));
                scriptModule.registerScriptContext(new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"));
                scriptModule.registerScriptContext(new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"));
            }
        }
}
