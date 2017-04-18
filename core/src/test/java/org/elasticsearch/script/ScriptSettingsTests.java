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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ScriptSettingsTests extends ESTestCase {

    public void testSettingsAreProperlyPropogated() {
        ScriptEngineRegistry scriptEngineRegistry =
            new ScriptEngineRegistry(Collections.singletonList(new CustomScriptEngineService()));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.emptyList());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        boolean enabled = randomBoolean();
        Settings s = Settings.builder().put("script.inline", enabled).build();
        for (Iterator<Setting<Boolean>> iter = scriptSettings.getScriptLanguageSettings().iterator(); iter.hasNext();) {
            Setting<Boolean> setting = iter.next();
            if (setting.getKey().endsWith(".inline")) {
                assertThat("inline settings should have propagated", setting.get(s), equalTo(enabled));
                assertThat(setting.getDefaultRaw(s), equalTo(Boolean.toString(enabled)));
            }
        }
    }

    private static class CustomScriptEngineService implements ScriptEngineService {

        public static final String NAME = "custom";

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        public String getExtension() {
            return NAME;
        }

        @Override
        public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
            return null;
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public void close() {
        }
    }

}
