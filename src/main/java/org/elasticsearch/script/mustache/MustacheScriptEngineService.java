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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.io.UTF8StreamWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.lang.ref.SoftReference;
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
public class MustacheScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "mustache";

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
    @Inject
    public MustacheScriptEngineService(Settings settings) {
        super(settings);
    }

    /**
     * Compile a template string to (in this case) a Mustache object than can
     * later be re-used for execution to fill in missing parameter values.
     *
     * @param template
     *            a string representing the template to compile.
     * @return a compiled template object for later execution.
     * */
    @Override
    public Object compile(String template) {
        /** Factory to generate Mustache objects from. */
        return (new JsonEscapingMustacheFactory()).compile(new FastStringReader(template), "query-template");
    }

    /**
     * Execute a compiled template object (as retrieved from the compile method)
     * and fill potential place holders with the variables given.
     *
     * @param template
     *            compiled template object.
     * @param vars
     *            map of variables to use during substitution.
     *
     * @return the processed string with all given variables substitued.
     * */
    @Override
    public Object execute(Object template, Map<String, Object> vars) {
        BytesStreamOutput result = new BytesStreamOutput();
        UTF8StreamWriter writer = utf8StreamWriter().setOutput(result);
        ((Mustache) template).execute(writer, vars);
        try {
            writer.flush();
        } catch (IOException e) {
            logger.error("Could not execute query template (failed to flush writer): ", e);
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error("Could not execute query template (failed to close writer): ", e);
            }
        }
        return result.bytes();
    }

    @Override
    public String[] types() {
        return new String[] {NAME};
    }

    @Override
    public String[] extensions() {
        return new String[] {NAME};
    }

    @Override
    public boolean sandboxed() {
        return true;
    }

    @Override
    public ExecutableScript executable(Object mustache,
            @Nullable Map<String, Object> vars) {
        return new MustacheExecutableScript((Mustache) mustache, vars);
    }

    @Override
    public SearchScript search(Object compiledScript, SearchLookup lookup,
            @Nullable Map<String, Object> vars) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object unwrap(Object value) {
        return value;
    }

    @Override
    public void close() {
        // Nothing to do here
    }

    @Override
    public void scriptRemoved(CompiledScript script) {
        // Nothing to do here
    }

    /**
     * Used at query execution time by script service in order to execute a query template.
     * */
    private class MustacheExecutableScript implements ExecutableScript {
        /** Compiled template object. */
        private Mustache mustache;
        /** Parameters to fill above object with. */
        private Map<String, Object> vars;

        /**
         * @param mustache the compiled template object
         * @param vars the parameters to fill above object with
         **/
        public MustacheExecutableScript(Mustache mustache,
                Map<String, Object> vars) {
            this.mustache = mustache;
            this.vars = vars == null ? Collections.<String, Object>emptyMap() : vars;
        }

        @Override
        public void setNextVar(String name, Object value) {
            this.vars.put(name, value);
        }

        @Override
        public Object run() {
            BytesStreamOutput result = new BytesStreamOutput();
            UTF8StreamWriter writer = utf8StreamWriter().setOutput(result);
            mustache.execute(writer, vars);
            try {
                writer.flush();
            } catch (IOException e) {
                logger.error("Could not execute query template (failed to flush writer): ", e);
            } finally {
                try {
                    writer.close();
                } catch (IOException e) {
                    logger.error("Could not execute query template (failed to close writer): ", e);
                }
            }
            return result.bytes();
        }

        @Override
        public Object unwrap(Object value) {
            return value;
        }
    }
}
