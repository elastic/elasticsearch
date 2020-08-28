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
import org.elasticsearch.node.MockNode;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class MockScriptService extends ScriptService {
    /**
     * Marker plugin used by {@link MockNode} to enable {@link MockScriptService}.
     */
    public static class TestPlugin extends Plugin {}

    public MockScriptService(Settings settings, Map<String, ScriptEngine> engines, Map<String, ScriptContext<?>> contexts) {
        super(settings, engines, contexts);
    }

    @Override
    boolean compilationLimitsEnabled() {
        return false;
    }

    public static <T> MockScriptService singleContext(ScriptContext<T> context, Function<String, T> compile,
                                                      Map<String, StoredScriptSource> storedLookup) {
        ScriptEngine engine = new ScriptEngine() {
            @Override
            public String getType() {
                return "lang";
            }

            @Override
            public <FactoryType> FactoryType compile(String name, String code, ScriptContext<FactoryType> context,
                                                     Map<String, String> params) {
                return context.factoryClazz.cast(compile.apply(code));
            }

            @Override
            public Set<ScriptContext<?>> getSupportedContexts() {
                return Set.of(context);
            }
        };
        return new MockScriptService(Settings.EMPTY, Map.of("lang", engine), Map.of(context.name, context)) {
            @Override
            protected StoredScriptSource getScriptFromClusterState(String id) {
                return storedLookup.get(id);
            }
        };
    }
}
