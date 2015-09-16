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
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class NativeScriptTests extends ESTestCase {

    @Test
    public void testNativeScript() throws InterruptedException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        Settings settings = Settings.settingsBuilder()
                .put("name", "testNativeScript")
                .put("path.home", createTempDir())
                .build();
        ScriptModule scriptModule = new ScriptModule(settings);
        scriptModule.registerScript("my", MyNativeScriptFactory.class);
        Injector injector = new ModulesBuilder().add(
                new EnvironmentModule(new Environment(settings)),
                new ThreadPoolModule(new ThreadPool(settings)),
                new SettingsModule(settings),
                scriptModule).createInjector();

        ScriptService scriptService = injector.getInstance(ScriptService.class);

        ExecutableScript executable = scriptService.executable(new Script("my", ScriptType.INLINE, NativeScriptEngineService.NAME, null),
                ScriptContext.Standard.SEARCH, contextAndHeaders);
        assertThat(executable.run().toString(), equalTo("test"));
        terminate(injector.getInstance(ThreadPool.class));
    }

    @Test
    public void testFineGrainedSettingsDontAffectNativeScripts() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        Settings.Builder builder = Settings.settingsBuilder();
        if (randomBoolean()) {
            ScriptType scriptType = randomFrom(ScriptType.values());
            builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + scriptType, randomFrom(ScriptMode.values()));
        } else {
            String scriptContext = randomFrom(ScriptContext.Standard.values()).getKey();
            builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + scriptContext, randomFrom(ScriptMode.values()));
        }
        Settings settings = builder.put("path.home", createTempDir()).build();
        Environment environment = new Environment(settings);
        ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, null);
        Map<String, NativeScriptFactory> nativeScriptFactoryMap = new HashMap<>();
        nativeScriptFactoryMap.put("my", new MyNativeScriptFactory());
        Set<ScriptEngineService> scriptEngineServices = ImmutableSet.<ScriptEngineService>of(new NativeScriptEngineService(settings, nativeScriptFactoryMap));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(new ArrayList<ScriptContext.Plugin>());
        ScriptService scriptService = new ScriptService(settings, environment, scriptEngineServices, resourceWatcherService, scriptContextRegistry);

        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            assertThat(scriptService.compile(new Script("my", ScriptType.INLINE, NativeScriptEngineService.NAME, null), scriptContext,
                    contextAndHeaders), notNullValue());
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