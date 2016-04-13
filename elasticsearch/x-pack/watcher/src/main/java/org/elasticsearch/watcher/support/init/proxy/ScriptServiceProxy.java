/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.init.proxy;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.shield.SecurityContext;
import org.elasticsearch.shield.user.XPackUser;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.xpack.common.init.LazyInitializable;

import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 *A lazily initialized proxy to the elasticsearch {@link ScriptService}. Inject this proxy whenever the script
 * service needs to be injected to avoid circular dependencies issues.
 */
public class ScriptServiceProxy implements LazyInitializable {

    private ScriptService service;
    private SecurityContext securityContext;

    /**
     * Creates a proxy to the given script service (can be used for testing)
     */
    public static ScriptServiceProxy of(ScriptService service) {
        ScriptServiceProxy proxy = new ScriptServiceProxy();
        proxy.service = service;
        proxy.securityContext = SecurityContext.Insecure.INSTANCE;
        return proxy;
    }

    @Override
    public void init(Injector injector) {
        this.service = injector.getInstance(ScriptService.class);
        this.securityContext = injector.getInstance(SecurityContext.class);
    }

    public CompiledScript compile(Script script) {
        return securityContext.executeAs(XPackUser.INSTANCE, () ->
            compile(new org.elasticsearch.script.Script(script.script(), script.type(), script.lang(), script.params()), emptyMap()));
    }

    public CompiledScript compile(org.elasticsearch.script.Script script, Map<String, String> compileParams) {
        return securityContext.executeAs(XPackUser.INSTANCE, () ->
                service.compile(script, WatcherScriptContext.CTX, compileParams));
    }

    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        return securityContext.executeAs(XPackUser.INSTANCE, () ->
                service.executable(compiledScript, vars));
    }


    public ExecutableScript executable(org.elasticsearch.script.Script script) {
        return securityContext.executeAs(XPackUser.INSTANCE, () ->
                service.executable(script, WatcherScriptContext.CTX, emptyMap()));
    }

    public static final ScriptContext.Plugin INSTANCE = new ScriptContext.Plugin("xpack", "watch");

    private static class WatcherScriptContext implements ScriptContext {

        public static final ScriptContext CTX = new WatcherScriptContext();

        @Override
        public String getKey() {
            return INSTANCE.getKey();
        }
    }
}
