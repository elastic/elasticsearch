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
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class NativeScriptTests extends ESTestCase {
    public void testNativeScript() throws InterruptedException {
        Settings settings = Settings.builder()
                .put("node.name", "testNativeScript")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();
        SettingsModule settingsModule = new SettingsModule(settings);
        ScriptModule scriptModule = new ScriptModule();
        scriptModule.prepareSettings(settingsModule);
        scriptModule.registerScript("my", MyNativeScriptFactory.class);
        Injector injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                new ThreadPoolModule(new ThreadPool(settings)),
                new SettingsModule(settings),
                scriptModule).createInjector();

        ScriptService scriptService = injector.getInstance(ScriptService.class);

        ExecutableScript executable = scriptService.executable(new Script("my", ScriptType.INLINE, NativeScriptEngineService.NAME, null),
                ScriptContext.Standard.SEARCH, Collections.emptyMap());
        assertThat(executable.run().toString(), equalTo("test"));
        terminate(injector.getInstance(ThreadPool.class));
    }

    public void testFineGrainedSettingsDontAffectNativeScripts() throws IOException {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            ScriptType scriptType = randomFrom(ScriptType.values());
            builder.put("script" + "." + scriptType.getScriptType(), randomFrom(ScriptMode.values()));
        } else {
            ScriptContext scriptContext = randomFrom(ScriptContext.Standard.values());
            builder.put("script" + "." + scriptContext.getKey(), randomFrom(ScriptMode.values()));
        }
        Settings settings = builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = new Environment(settings);
        ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, null);
        Map<String, NativeScriptFactory> nativeScriptFactoryMap = new HashMap<>();
        nativeScriptFactoryMap.put("my", new MyNativeScriptFactory());
        Set<ScriptEngineService> scriptEngineServices = singleton(new NativeScriptEngineService(settings, nativeScriptFactoryMap));
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singletonList(new ScriptEngineRegistry.ScriptEngineRegistration(NativeScriptEngineService.class, NativeScriptEngineService.TYPES)));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(new ArrayList<>());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        ScriptService scriptService = new ScriptService(settings, environment, scriptEngineServices, resourceWatcherService, scriptEngineRegistry, scriptContextRegistry, scriptSettings);

        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            assertThat(scriptService.compile(new Script("my", ScriptType.INLINE, NativeScriptEngineService.NAME, null), scriptContext,
                    Collections.emptyMap()), notNullValue());
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
    }

    static class MyScript extends AbstractExecutableScript {
        @Override
        public Object run() {
            return "test";
        }
    }
}
