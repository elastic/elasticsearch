/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        super(settings, engines, contexts, () -> 1L);
    }

    @Override
    boolean compilationLimitsEnabled() {
        return false;
    }

    public static <T> MockScriptService singleContext(
        ScriptContext<T> context,
        Function<String, T> compile,
        Map<String, StoredScriptSource> storedLookup
    ) {
        ScriptEngine engine = new ScriptEngine() {
            @Override
            public String getType() {
                return "lang";
            }

            @Override
            public <FactoryType> FactoryType compile(
                String name,
                String code,
                ScriptContext<FactoryType> context,
                Map<String, String> params
            ) {
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
