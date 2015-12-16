/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.text.xmustache;

import com.github.mustachejava.Mustache;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.io.UTF8StreamWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.Locale;
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
    public Object compile(String template, Map<String, String> params) {
        /** Factory to generate Mustache objects from. */
        XContentType xContentType = detectContentType(template);
        template = trimContentType(template);
        return (new XMustacheFactory(xContentType)).compile(new FastStringReader(template), "query-template");
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
    public ExecutableScript executable(CompiledScript compiledScript,
                                       @Nullable Map<String, Object> vars) {
        return new MustacheExecutableScript((Mustache) compiledScript.compiled(), vars);
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

    public static String prepareTemplate(String template, @Nullable XContentType contentType) {
        if (contentType == null) {
            return template;
        }
        return new StringBuilder("__")
                .append(contentType.shortName().toLowerCase(Locale.ROOT))
                .append("__::")
                .append(template)
                .toString();
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

    private String trimContentType(String template) {
        if (!template.startsWith("__")){
            return template; //Doesn't even start with __ so can't have a content type
        }
        int index = template.indexOf("__::", 3); //There must be a __<content_type__:: prefix so the minimum length before detecting '__::' is 3
        if (index >= 0 && index < 12) { //Assume that the content type name is less than 10 characters long otherwise we may falsely detect strings that start with '__ and have '__::' somewhere in the content
            if (template.length() == 6) {
                template = "";
            } else {
                template = template.substring(index + 4);
            }
        }
        return template;
    }

    private XContentType detectContentType(String template) {
        if (template.startsWith("__")) {
            int endOfContentName = template.indexOf("__::", 3); //There must be a __<content_type__:: prefix so the minimum length before detecting '__::' is 3
            if (endOfContentName != -1) {
                return XContentType.fromRestContentType(template.substring(2, endOfContentName));
            }
        }
        return null;
    }

}
