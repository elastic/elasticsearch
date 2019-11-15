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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScriptLanguagesInfoTests extends ESTestCase {
    // Test empty types allowed, INLINE only, STORED only, INLINE and STORED - DONE
    // Test empty contexts allowed with different engines, search only, update only, search and update
    // Test expression, mustache, painless and empty engines

    public void testEmptyTypesAllowedReturnsAllTypes() {
        ScriptService ss = getMockScriptService(Settings.EMPTY);
        ScriptLanguagesInfo info = ss.getScriptLanguages();
        ScriptType[] types = ScriptType.values();
        assertEquals(types.length, info.typesAllowed.size());
        for(ScriptType type: types) {
            assertTrue("[" + type.getName() + "] is allowed", info.typesAllowed.contains(type.getName()));
        }
    }

    public void testSingleTypesAllowedReturnsThatType() {
        for (ScriptType type: ScriptType.values()) {
            ScriptService ss = getMockScriptService(
                Settings.builder().put("script.allowed_types", type.getName()).build()
            );
            ScriptLanguagesInfo info = ss.getScriptLanguages();
            assertEquals(1, info.typesAllowed.size());
            assertTrue("[" + type.getName() + "] is allowed", info.typesAllowed.contains(type.getName()));
        }
    }

    public void testBothTypesAllowedReturnsBothTypes() {
        List<String> types = Arrays.stream(ScriptType.values()).map(ScriptType::getName).collect(Collectors.toList());
        Settings.Builder settings = Settings.builder().putList("script.allowed_types", types);
        ScriptService ss = getMockScriptService(settings.build());
        ScriptLanguagesInfo info = ss.getScriptLanguages();
        assertEquals(types.size(), info.typesAllowed.size());
        for(String type: types) {
            assertTrue("[" + type + "] is allowed", info.typesAllowed.contains(type));
        }
    }

    private ScriptService getMockScriptService(Settings settings) {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME,
            Collections.singletonMap("test_script", script -> 1),
            Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(settings, engines, ScriptModule.CORE_CONTEXTS);
    }
}
