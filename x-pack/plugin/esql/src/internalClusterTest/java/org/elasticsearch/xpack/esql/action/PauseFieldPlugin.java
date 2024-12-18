/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class PauseFieldPlugin extends Plugin implements ScriptPlugin {
    public static CountDownLatch startEmitting = new CountDownLatch(1);
    public static CountDownLatch allowEmitting = new CountDownLatch(1);

    public static void resetPlugin() {
        PauseFieldPlugin.allowEmitting = new CountDownLatch(1);
        PauseFieldPlugin.startEmitting = new CountDownLatch(1);
    }

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new ScriptEngine() {
            @Override
            public String getType() {
                return "pause";
            }

            @Override
            @SuppressWarnings("unchecked")
            public <FactoryType> FactoryType compile(
                String name,
                String code,
                ScriptContext<FactoryType> context,
                Map<String, String> params
            ) {
                if (context == LongFieldScript.CONTEXT) {
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
                                    startEmitting.countDown();
                                    try {
                                        assertTrue(allowEmitting.await(30, TimeUnit.SECONDS));
                                    } catch (InterruptedException e) {
                                        throw new AssertionError(e);
                                    }
                                    emit(1);
                                }
                            };
                        }
                    };
                }
                throw new IllegalStateException("unsupported type " + context);
            }

            @Override
            public Set<ScriptContext<?>> getSupportedContexts() {
                return Set.of(LongFieldScript.CONTEXT);
            }
        };
    }

}
