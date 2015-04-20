/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template.xmustache;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.io.UTF8StreamWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.mustache.Mustache;
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
 *
 */
public class XMustacheScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "xmustache";

    /**
     * @param settings automatically wired by Guice.
     * */
    @Inject
    public XMustacheScriptEngineService(Settings settings) {
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
        return (new XMustacheFactory()).compile(new FastStringReader(template), "query-template");
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
        return new String[] { NAME };
    }

    @Override
    public String[] extensions() {
        return new String[] { NAME };
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
}
