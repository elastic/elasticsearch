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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class NativeScriptTests extends ElasticsearchTestCase {

    @Test
    public void testNativeScript() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("script.native.my.type", MyNativeScriptFactory.class.getName())
                .put("name", "testNativeScript")
                .build();
        Injector injector = new ModulesBuilder().add(
                new SettingsModule(settings),
                new ScriptModule(settings)).createInjector();

        ScriptService scriptService = injector.getInstance(ScriptService.class);

        ExecutableScript executable = scriptService.executable("native", "my", null);
        assertThat(executable.run().toString(), equalTo("test"));
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