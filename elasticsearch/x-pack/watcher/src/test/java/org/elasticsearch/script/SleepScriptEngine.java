/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.script;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A dummy script engine used for testing. Scripts must be a number. Running the script
 */
public class SleepScriptEngine implements ScriptEngineService {

    public static final String NAME = "sleep";

    public static final List<String> TYPES = Collections.singletonList(NAME);

    public static class TestPlugin extends Plugin {

        public TestPlugin() {
        }

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public String description() {
            return "Mock script engine for integration tests";
        }

        public void onModule(ScriptModule module) {
            module.addScriptEngine(new ScriptEngineRegistry.ScriptEngineRegistration(SleepScriptEngine.class, SleepScriptEngine.TYPES));
        }

    }

    @Override
    public List<String> getTypes() {
        return TYPES;
    }

    @Override
    public List<String> getExtensions() {
        return TYPES;
    }

    @Override
    public boolean isSandboxed() {
        return true;
    }

    @Override
    public Object compile(String script, Map<String, String> params) {
        return script;
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
        return new AbstractSearchScript() {
            @Override
            public Object run() {
                try {
                    Thread.sleep(((Number) vars.get("millis")).longValue());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
        };
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        return null;
    }

    @Override
    public void scriptRemoved(@Nullable CompiledScript script) {
    }

    @Override
    public void close() throws IOException {
    }

    public static org.elasticsearch.watcher.support.Script sleepScript(long millis) {
        return new org.elasticsearch.watcher.support.Script.Builder.Inline("")
                .lang("sleep")
                .params(Collections.singletonMap("millis", millis)).build();
    }

}
