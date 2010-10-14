/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.script.groovy;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (shay.banon)
 */
public class GroovyScriptEngineService extends AbstractComponent implements ScriptEngineService {

    private final AtomicLong counter = new AtomicLong();

    private final GroovyClassLoader loader;

    @Inject public GroovyScriptEngineService(Settings settings) {
        super(settings);
        this.loader = new GroovyClassLoader(settings.getClassLoader());
    }

    @Override public void close() {
        loader.clearCache();
    }

    @Override public String[] types() {
        return new String[]{"groovy"};
    }

    @Override public String[] extensions() {
        return new String[]{"groovy"};
    }

    @Override public Object compile(String script) {
        return loader.parseClass(script, generateScriptName());
    }

    @Override public ExecutableScript executable(Object compiledScript, Map<String, Object> vars) {
        try {
            Class scriptClass = (Class) compiledScript;
            Script scriptObject = (Script) scriptClass.newInstance();
            Binding binding = new Binding();
            if (vars != null) {
                binding.getVariables().putAll(vars);
            }
            scriptObject.setBinding(binding);
            return new GroovyExecutableScript(scriptObject);
        } catch (Exception e) {
            throw new ScriptException("failed to build executable script", e);
        }
    }

    @Override public Object execute(Object compiledScript, Map<String, Object> vars) {
        try {
            Class scriptClass = (Class) compiledScript;
            Script scriptObject = (Script) scriptClass.newInstance();
            Binding binding = new Binding(vars);
            scriptObject.setBinding(binding);
            return scriptObject.run();
        } catch (Exception e) {
            throw new ScriptException("failed to execute script", e);
        }
    }

    @Override public Object unwrap(Object value) {
        return value;
    }

    private String generateScriptName() {
        return "Script" + counter.incrementAndGet() + ".groovy";
    }

    public static class GroovyExecutableScript implements ExecutableScript {

        private final Script script;

        public GroovyExecutableScript(Script script) {
            this.script = script;
        }

        @Override public Object run() {
            return script.run();
        }

        @Override public Object run(Map<String, Object> vars) {
            script.getBinding().getVariables().putAll(vars);
            return script.run();
        }

        @Override public Object unwrap(Object value) {
            return value;
        }
    }
}
