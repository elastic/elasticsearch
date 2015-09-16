/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.init.proxy;

import org.elasticsearch.common.ContextAndHeaderHolder;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.script.*;
import org.elasticsearch.watcher.shield.ShieldIntegration;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.init.InitializingService;

import java.util.Map;

/**
 *A lazily initialized proxy to the elasticsearch {@link ScriptService}. Inject this proxy whenever the script
 * service needs to be injected to avoid circular dependencies issues.
 */
public class ScriptServiceProxy implements InitializingService.Initializable {

    private ScriptService service;
    private ContextAndHeaderHolder contextAndHeaderHolder = new ContextAndHeaderHolder();

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
        ShieldIntegration shieldIntegration = injector.getInstance(ShieldIntegration.class);
        if (shieldIntegration != null) {
            shieldIntegration.putUserInContext(contextAndHeaderHolder);
        }
    }

    public CompiledScript compile(Script script) {
        return compile(new org.elasticsearch.script.Script(script.script(), script.type(), script.lang(), script.params()));
    }

    public CompiledScript compile(org.elasticsearch.script.Script script) {
        return service.compile(script, WatcherScriptContext.CTX, contextAndHeaderHolder);
    }

    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        return service.executable(compiledScript, vars);
    }


    public ExecutableScript executable(org.elasticsearch.script.Script script) {
        return service.executable(script, WatcherScriptContext.CTX, contextAndHeaderHolder);
    }

    public static final ScriptContext.Plugin INSTANCE = new ScriptContext.Plugin("elasticsearch-watcher", "watch");

    private static class WatcherScriptContext implements ScriptContext {
        public static final ScriptContext CTX = new WatcherScriptContext();
        @Override
        public String getKey() {
            return INSTANCE.getKey();
        }
    }
}
