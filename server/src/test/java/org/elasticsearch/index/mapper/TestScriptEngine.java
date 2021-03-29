/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;

import java.util.Map;
import java.util.Set;

public abstract class TestScriptEngine implements ScriptEngine {
    public static <F> ScriptService scriptService(ScriptContext<F> context, F factory) {
        return new ScriptService(Settings.EMPTY, Map.of("test", new TestScriptEngine() {
            @Override
            protected Object buildScriptFactory(ScriptContext<?> context) {
                return factory;
            }

            @Override
            public Set<ScriptContext<?>> getSupportedContexts() {
                return Set.of(context);
            }
        }), Map.of(context.name, context));
    }

    @Override
    public final String getType() {
        return "test";
    }

    @Override
    public final <FactoryType> FactoryType compile(
        String name,
        String code,
        ScriptContext<FactoryType> context,
        Map<String, String> params
    ) {
        @SuppressWarnings("unchecked")
        FactoryType castFactory = (FactoryType) buildScriptFactory(context);
        return castFactory;
    }

    protected abstract Object buildScriptFactory(ScriptContext<?> context);
}
