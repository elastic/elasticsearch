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

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.io.UTF8StreamWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.Reader;
import java.lang.ref.SoftReference;
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
public final class MustacheScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "mustache";

    static final String CONTENT_TYPE_PARAM = "content_type";
    static final String JSON_CONTENT_TYPE = "application/json";
    static final String PLAIN_TEXT_CONTENT_TYPE = "text/plain";

    /** Thread local UTF8StreamWriter to store template execution results in, thread local to save object creation.*/
    private static ThreadLocal<SoftReference<UTF8StreamWriter>> utf8StreamWriter = new ThreadLocal<>();

    /** If exists, reset and return, otherwise create, reset and return a writer.*/
    private static UTF8StreamWriter utf8StreamWriter() {
        SoftReference<UTF8StreamWriter> ref = utf8StreamWriter.get();
        UTF8StreamWriter writer = (ref == null) ? null : ref.get();
        if (writer == null) {
            writer = new UTF8StreamWriter(1024 * 4);
            utf8StreamWriter.set(new SoftReference<>(writer));
        }
        writer.reset();
        return writer;
    }

    /**
     * @param settings automatically wired by Guice.
     * */
    public MustacheScriptEngineService(Settings settings) {
        super(settings);
    }

    /**
     * Compile a template string to (in this case) a Mustache object than can
     * later be re-used for execution to fill in missing parameter values.
     *
     * @param templateSource
     *            a string representing the template to compile.
     * @return a compiled template object for later execution.
     * */
    @Override
    public Object compile(String templateName, String templateSource, Map<String, String> params) {
        String contentType = params.getOrDefault(CONTENT_TYPE_PARAM, JSON_CONTENT_TYPE);
        final DefaultMustacheFactory mustacheFactory;
        switch (contentType){
            case PLAIN_TEXT_CONTENT_TYPE:
                mustacheFactory = new NoneEscapingMustacheFactory();
                break;
            case JSON_CONTENT_TYPE:
            default:
                // assume that the default is json encoding:
                mustacheFactory = new JsonEscapingMustacheFactory();
                break;
        }
        mustacheFactory.setObjectHandler(new CustomReflectionObjectHandler());
        Reader reader = new FastStringReader(templateSource);
        return mustacheFactory.compile(reader, "query-template");
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public String getExtension() {
        return NAME;
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript,
            @Nullable Map<String, Object> vars) {
        return new MustacheExecutableScript(compiledScript, vars);
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup,
            @Nullable Map<String, Object> vars) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // Nothing to do here
    }

    @Override
    public void scriptRemoved(CompiledScript script) {
        // Nothing to do here
    }

    // permission checked before doing crazy reflection
    static final SpecialPermission SPECIAL_PERMISSION = new SpecialPermission();

    /**
     * Used at query execution time by script service in order to execute a query template.
     * */
    private class MustacheExecutableScript implements ExecutableScript {
        /** Compiled template object wrapper. */
        private CompiledScript template;
        /** Parameters to fill above object with. */
        private Map<String, Object> vars;

        /**
         * @param template the compiled template object wrapper
         * @param vars the parameters to fill above object with
         **/
        public MustacheExecutableScript(CompiledScript template, Map<String, Object> vars) {
            this.template = template;
            this.vars = vars == null ? Collections.<String, Object>emptyMap() : vars;
        }

        @Override
        public void setNextVar(String name, Object value) {
            this.vars.put(name, value);
        }

        @Override
        public Object run() {
            final BytesStreamOutput result = new BytesStreamOutput();
            try (UTF8StreamWriter writer = utf8StreamWriter().setOutput(result)) {
                // crazy reflection here
                SecurityManager sm = System.getSecurityManager();
                if (sm != null) {
                    sm.checkPermission(SPECIAL_PERMISSION);
                }
                AccessController.doPrivileged(new PrivilegedAction<Void>() {
                    @Override
                    public Void run() {
                        ((Mustache) template.compiled()).execute(writer, vars);
                        return null;
                    }
                });
            } catch (Exception e) {
                logger.error("Error running {}", e, template);
                throw new GeneralScriptException("Error running " + template, e);
            }
            return result.bytes();
        }
    }

    @Override
    public boolean isInlineScriptEnabled() {
        return true;
    }
}
