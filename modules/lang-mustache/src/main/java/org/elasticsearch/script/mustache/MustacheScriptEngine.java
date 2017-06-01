/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.script.mustache;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.io.Reader;
import java.io.StringWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Map;

/**
 * Main entry point handling template registration, compilation and
 * execution.
 *
 * Template handling is based on Mustache. Template handling is a two step
 * process: First compile the string representing the template, the resulting
 * {@link Mustache} object can then be re-used for subsequent executions.
 */
public final class MustacheScriptEngine implements ScriptEngine {
    private static final Logger logger = ESLoggerFactory.getLogger(MustacheScriptEngine.class);

    public static final String NAME = "mustache";

    /**
     * Compile a template string to (in this case) a Mustache object than can
     * later be re-used for execution to fill in missing parameter values.
     *
     * @param templateSource a string representing the template to compile.
     * @return a compiled template object for later execution.
     * */
    @Override
    public <T> T compile(String templateName, String templateSource, ScriptContext<T> context, Map<String, String> params) {
        if (context.instanceClazz.equals(ExecutableScript.class) == false) {
            throw new IllegalArgumentException("mustache engine does not know how to handle context [" + context.name + "]");
        }
        final MustacheFactory factory = createMustacheFactory(params);
        Reader reader = new FastStringReader(templateSource);
        Mustache template = factory.compile(reader, "query-template");
        ExecutableScript.Factory compiled = p -> new MustacheExecutableScript(template, p);
        return context.factoryClazz.cast(compiled);
    }

    private CustomMustacheFactory createMustacheFactory(Map<String, String> params) {
        if (params == null || params.isEmpty() || params.containsKey(Script.CONTENT_TYPE_OPTION) == false) {
            return new CustomMustacheFactory();
        }
        return new CustomMustacheFactory(params.get(Script.CONTENT_TYPE_OPTION));
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Used at query execution time by script service in order to execute a query template.
     * */
    private class MustacheExecutableScript implements ExecutableScript {
        /** Factory template. */
        private Mustache template;
        /** Parameters to fill above object with. */
        private Map<String, Object> vars;

        /**
         * @param template the compiled template object wrapper
         * @param vars the parameters to fill above object with
         **/
        MustacheExecutableScript(Mustache template, Map<String, Object> vars) {
            this.template = template;
            this.vars = vars == null ? Collections.emptyMap() : vars;
        }

        @Override
        public void setNextVar(String name, Object value) {
            this.vars.put(name, value);
        }

        @Override
        public Object run() {
            final StringWriter writer = new StringWriter();
            try {
                // crazy reflection here
                SpecialPermission.check();
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    template.execute(writer, vars);
                    return null;
                });
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("Error running {}", template), e);
                throw new GeneralScriptException("Error running " + template, e);
            }
            return writer.toString();
        }
    }
}
