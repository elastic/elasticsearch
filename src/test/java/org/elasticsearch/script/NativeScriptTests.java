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
import com.google.common.collect.Lists;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class NativeScriptTests extends ElasticsearchTestCase {

    @Test
    public void testNativeScript() throws InterruptedException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("script.native.my.type", MyNativeScriptFactory.class.getName())
                .put("name", "testNativeScript")
                .build();
        Injector injector = new ModulesBuilder().add(
                new ThreadPoolModule(settings),
                new SettingsModule(settings),
                new ScriptModule(settings)).createInjector();

        ScriptService scriptService = injector.getInstance(ScriptService.class);

        ExecutableScript executable = scriptService.executable(new Script(NativeScriptEngineService.NAME, "my", ScriptType.INLINE, null), ScriptContext.Standard.SEARCH);
        assertThat(executable.run().toString(), equalTo("test"));
        terminate(injector.getInstance(ThreadPool.class));
    }

    @Test
    public void testFineGrainedSettingsDontAffectNativeScripts() throws IOException {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
        if (randomBoolean()) {
            ScriptType scriptType = randomFrom(ScriptType.values());
            builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + scriptType, randomFrom(ScriptMode.values()));
        } else {
            String scriptContext = randomFrom(ScriptContext.Standard.values()).getKey();
            builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + scriptContext, randomFrom(ScriptMode.values()));
        }
        Settings settings = builder.build();
        Environment environment = new Environment(settings);
        ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, null);
        Map<String, NativeScriptFactory> nativeScriptFactoryMap = new HashMap<>();
        nativeScriptFactoryMap.put("my", new MyNativeScriptFactory());
        Set<ScriptEngineService> scriptEngineServices = ImmutableSet.<ScriptEngineService>of(new NativeScriptEngineService(settings, nativeScriptFactoryMap));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Lists.<ScriptContext.Plugin>newArrayList());
        ScriptService scriptService = new ScriptService(settings, environment, scriptEngineServices, resourceWatcherService, scriptContextRegistry);

        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            assertThat(scriptService.compile(new Script(NativeScriptEngineService.NAME, "my", ScriptType.INLINE, null), scriptContext), notNullValue());
        }
    }

    static class MyNativeScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new MyScript();
        }
    }

    static class MyScript extends AbstractExecutableScript {
        @Override
        public Object run() {
            return "test";
        }
    }
}