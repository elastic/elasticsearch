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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.watcher.ResourceWatcherService;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class NativeScriptTests extends ESTestCase {
    public void testNativeScript() throws InterruptedException {
        Settings settings = Settings.builder()
                .put("node.name", "testNativeScript")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
                .build();
        ScriptModule scriptModule = new ScriptModule(settings, new Environment(settings), null,
                singletonList(new NativeScriptEngineService(settings, singletonMap("my", new MyNativeScriptFactory()))), emptyList());
        List<Setting<?>> scriptSettings = scriptModule.getSettings();
        scriptSettings.add(InternalSettingsPlugin.VERSION_CREATED);

        ExecutableScript executable = scriptModule.getScriptService().executable(
                new Script(ScriptType.INLINE, NativeScriptEngineService.NAME, "my", Collections.emptyMap()), ScriptContext.Standard.SEARCH);
        assertThat(executable.run().toString(), equalTo("test"));
    }

    public void testFineGrainedSettingsDontAffectNativeScripts() throws IOException {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            ScriptType scriptType = randomFrom(ScriptType.values());
            builder.put("script" + "." + scriptType.getName(), randomBoolean());
        } else {
            ScriptContext scriptContext = randomFrom(ScriptContext.Standard.values());
            builder.put("script" + "." + scriptContext.getKey(), randomBoolean());
        }
        Settings settings = builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = new Environment(settings);
        ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, null);
        Map<String, NativeScriptFactory> nativeScriptFactoryMap = new HashMap<>();
        nativeScriptFactoryMap.put("my", new MyNativeScriptFactory());
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singleton(new NativeScriptEngineService(settings,
            nativeScriptFactoryMap)));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(new ArrayList<>());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        ScriptService scriptService = new ScriptService(settings, environment, resourceWatcherService, scriptEngineRegistry,
            scriptContextRegistry, scriptSettings);

        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            assertThat(scriptService.compile(new Script(ScriptType.INLINE, NativeScriptEngineService.NAME, "my", Collections.emptyMap()),
                scriptContext), notNullValue());
        }
    }

    public static class MyNativeScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new MyScript();
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        @Override
        public String getName() {
            return "my";
        }
    }

    static class MyScript extends AbstractExecutableScript {
        @Override
        public Object run() {
            return "test";
        }
    }
}
