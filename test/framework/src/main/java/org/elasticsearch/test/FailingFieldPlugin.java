/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class FailingFieldPlugin extends Plugin implements ScriptPlugin {

    public static final String FAILING_FIELD_LANG = "failing_field";

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new ScriptEngine() {
            @Override
            public String getType() {
                return FAILING_FIELD_LANG;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <FactoryType> FactoryType compile(
                String name,
                String code,
                ScriptContext<FactoryType> context,
                Map<String, String> params
            ) {
                return (FactoryType) new LongFieldScript.Factory() {
                    @Override
                    public LongFieldScript.LeafFactory newFactory(
                        String fieldName,
                        Map<String, Object> params,
                        SearchLookup searchLookup,
                        OnScriptError onScriptError
                    ) {
                        return ctx -> new LongFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                            @Override
                            public void execute() {
                                throw new IllegalStateException("Accessing failing field");
                            }
                        };
                    }
                };
            }

            @Override
            public Set<ScriptContext<?>> getSupportedContexts() {
                return Set.of(LongFieldScript.CONTEXT);
            }
        };
    }
}
