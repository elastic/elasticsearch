/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.init.proxy;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.init.InitializingService;

import java.util.Map;

/**
 *A lazily initialized proxy to the elasticsearch {@link ScriptService}. Inject this proxy whenever the script
 * service needs to be injected to avoid circular dependencies issues.
 */
public class ScriptServiceProxy implements InitializingService.Initializable {

    private ScriptService service;

    /**
     * Creates a proxy to the given script service (can be used for testing)
     */
    public static ScriptServiceProxy of(ScriptService service) {
        ScriptServiceProxy proxy = new ScriptServiceProxy();
        proxy.service = service;
        return proxy;
    }

    @Override
    public void init(Injector injector) {
        this.service = injector.getInstance(ScriptService.class);
    }

    public CompiledScript compile(String lang, String script, ScriptService.ScriptType scriptType) {
        return service.compile(lang, script, scriptType);
    }

    public CompiledScript compile(Script script) {
        return compile(script.lang(), script.script(), script.type());
    }

    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        return service.executable(compiledScript, vars);
    }

    public ExecutableScript executable(Script script, Map<String, Object> vars) {
        if (script.params() != null && !script.params().isEmpty()) {
            vars = ImmutableMap.<String, Object>builder()
                    .putAll(script.params())
                    .putAll(vars)
                    .build();
        }
        return executable(script.lang(), script.script(), script.type(), vars);
    }

    public ExecutableScript executable(String lang, String script, ScriptService.ScriptType scriptType, Map<String, Object> vars) {
        return service.executable(lang, script, scriptType, vars);
    }

    public SearchScript search(SearchLookup lookup, String lang, String script, ScriptService.ScriptType scriptType, Map<String, Object> vars) {
        return service.search(lookup, lang, script, scriptType, vars);
    }
}
