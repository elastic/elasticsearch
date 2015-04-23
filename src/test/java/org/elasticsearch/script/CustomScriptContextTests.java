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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.script.expression.ExpressionScriptEngineService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;

public class CustomScriptContextTests extends ElasticsearchIntegrationTest {

    private static final ImmutableSet<String> LANG_SET = ImmutableSet.of(GroovyScriptEngineService.NAME, MustacheScriptEngineService.NAME, ExpressionScriptEngineService.NAME);

    private static final String PLUGIN_NAME = "testplugin";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", CustomScriptContextPlugin.class.getName())
                .put("script." + PLUGIN_NAME + "_custom_globally_disabled_op", "off")
                .put("script.engine.expression.inline." + PLUGIN_NAME + "_custom_exp_disabled_op", "off")
                        .build();
    }

    @Test
    public void testCustomScriptContextsSettings() {
        ScriptService scriptService = internalCluster().getInstance(ScriptService.class);
        for (String lang : LANG_SET) {
            for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
                try {
                    scriptService.compile(new Script(lang, "test", scriptType, null), new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"));
                    fail("script compilation should have been rejected");
                } catch(ScriptException e) {
                    assertThat(e.getMessage(), containsString("scripts of type [" + scriptType + "], operation [" + PLUGIN_NAME + "_custom_globally_disabled_op] and lang [" + lang + "] are disabled"));
                }
            }
        }

        try {
            scriptService.compile(new Script("expression", "1", ScriptService.ScriptType.INLINE, null), new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"));
            fail("script compilation should have been rejected");
        } catch(ScriptException e) {
            assertThat(e.getMessage(), containsString("scripts of type [inline], operation [" + PLUGIN_NAME + "_custom_exp_disabled_op] and lang [expression] are disabled"));
        }

        CompiledScript compiledScript = scriptService.compile(new Script("expression", "1", ScriptService.ScriptType.INLINE, null), randomFrom(ScriptContext.Standard.values()));
        assertThat(compiledScript, notNullValue());

        compiledScript = scriptService.compile(new Script("mustache", "1", ScriptService.ScriptType.INLINE, null), new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"));
        assertThat(compiledScript, notNullValue());

        for (String lang : LANG_SET) {
            compiledScript = scriptService.compile(new Script(lang, "1", ScriptService.ScriptType.INLINE, null), new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"));
            assertThat(compiledScript, notNullValue());
        }
    }

    @Test
    public void testCompileNonRegisteredPluginContext() {
        ScriptService scriptService = internalCluster().getInstance(ScriptService.class);
        try {
            scriptService.compile(new Script(randomFrom(LANG_SET.toArray(new String[LANG_SET.size()])), "test", randomFrom(ScriptService.ScriptType.values()), null), new ScriptContext.Plugin("test", "unknown"));
            fail("script compilation should have been rejected");
        } catch(ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("script context [test_unknown] not supported"));
        }
    }

    @Test
    public void testCompileNonRegisteredScriptContext() {
        ScriptService scriptService = internalCluster().getInstance(ScriptService.class);
        try {
            scriptService.compile(new Script(randomFrom(LANG_SET.toArray(new String[LANG_SET.size()])), "test", randomFrom(ScriptService.ScriptType.values()), null), new ScriptContext() {
                @Override
                public String getKey() {
                    return "test";
                }
            });
            fail("script compilation should have been rejected");
        } catch(ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("script context [test] not supported"));
        }
    }

    public static class CustomScriptContextPlugin extends AbstractPlugin {
        @Override
        public String name() {
            return "custom_script_context_plugin";
        }

        @Override
        public String description() {
            return "Custom script context plugin";
        }

        @Override
        public void processModule(Module module) {
            if (module instanceof ScriptModule) {
                ScriptModule scriptModule = (ScriptModule) module;
                scriptModule.registerScriptContext(new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"));
                scriptModule.registerScriptContext(new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"));
                scriptModule.registerScriptContext(new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"));
            }
        }
    }
}
