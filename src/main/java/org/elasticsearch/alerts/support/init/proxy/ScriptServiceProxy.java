/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.init.proxy;

import org.elasticsearch.alerts.support.init.InitializingService;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

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

    public ExecutableScript executable(String lang, String script, ScriptService.ScriptType scriptType, Map vars) {
        return service.executable(lang, script, scriptType, vars);
    }

    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
        return service.search(compiledScript, lookup, vars);
    }

    public SearchScript search(SearchLookup lookup, String lang, String script, ScriptService.ScriptType scriptType, Map<String, Object> vars) {
        return service.search(lookup, lang, script, scriptType, vars);
    }
}
