/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.script.mustache;

import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.MustacheFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.SizeLimitingStringWriter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.TemplateScript;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

/**
 * Main entry point handling template registration, compilation and
 * execution.
 *
 * Template handling is based on Mustache. Template handling is a two step
 * process: First compile the string representing the template, the resulting
 * {@link Mustache} object can then be re-used for subsequent executions.
 */
public final class MustacheScriptEngine implements ScriptEngine {
    /**
     * Compiler option to enable detection of missing parameters.
     */
    public static final String DETECT_MISSING_PARAMS_OPTION = "detect_missing_params";
    private static final Logger logger = LogManager.getLogger(MustacheScriptEngine.class);

    public static final String NAME = "mustache";

    public static final Setting<ByteSizeValue> MUSTACHE_RESULT_SIZE_LIMIT = new Setting<>(
        "mustache.max_output_size_bytes",
        s -> "1mb",
        s -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, "mustache.max_output_size_bytes"),
        Setting.Property.NodeScope
    );

    private final int sizeLimit;

    public MustacheScriptEngine(Settings settings) {
        sizeLimit = (int) MUSTACHE_RESULT_SIZE_LIMIT.get(settings).getBytes();
    }

    /**
     * Compile a template string to (in this case) a Mustache object than can
     * later be re-used for execution to fill in missing parameter values.
     *
     * @param templateSource a string representing the template to compile.
     * @return a compiled template object for later execution.
     * */
    @Override
    public <T> T compile(String templateName, String templateSource, ScriptContext<T> context, Map<String, String> options) {
        if (context.instanceClazz.equals(TemplateScript.class) == false) {
            throw new IllegalArgumentException("mustache engine does not know how to handle context [" + context.name + "]");
        }
        final MustacheFactory factory = createMustacheFactory(options);
        Reader reader = new StringReader(templateSource);
        try {
            Mustache template = factory.compile(reader, "query-template");
            TemplateScript.Factory compiled = params -> new MustacheExecutableScript(template, params);
            return context.factoryClazz.cast(compiled);
        } catch (MustacheException ex) {
            throw new ScriptException(ex.getMessage(), ex, Collections.emptyList(), templateSource, NAME);
        }

    }

    @Override
    public Set<ScriptContext<?>> getSupportedContexts() {
        return Set.of(TemplateScript.CONTEXT, TemplateScript.INGEST_CONTEXT);
    }

    private static CustomMustacheFactory createMustacheFactory(Map<String, String> options) {
        CustomMustacheFactory.Builder builder = CustomMustacheFactory.builder();
        if (options == null || options.isEmpty()) {
            return builder.build();
        }

        if (options.containsKey(Script.CONTENT_TYPE_OPTION)) {
            builder.mediaType(options.get(Script.CONTENT_TYPE_OPTION));
        }

        if (options.containsKey(DETECT_MISSING_PARAMS_OPTION)) {
            builder.detectMissingParams(Boolean.valueOf(options.get(DETECT_MISSING_PARAMS_OPTION)));
        }

        return builder.build();
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Used at query execution time by script service in order to execute a query template.
     * */
    private class MustacheExecutableScript extends TemplateScript {
        /** Factory template. */
        private Mustache template;

        private Map<String, Object> params;

        /**
         * @param template the compiled template object wrapper
         **/
        MustacheExecutableScript(Mustache template, Map<String, Object> params) {
            super(params);
            this.template = template;
            this.params = params;
        }

        @Override
        public String execute() {
            StringWriter writer = new SizeLimitingStringWriter(sizeLimit);
            try {
                template.execute(writer, params);
            } catch (Exception e) {
                // size limit exception can appear at several places in the causal list depending on script & context
                if (ExceptionsHelper.unwrap(e, SizeLimitingStringWriter.SizeLimitExceededException.class) != null) {
                    // don't log, client problem
                    throw new ElasticsearchParseException("Mustache script result size limit exceeded", e);
                }
                if (shouldLogException(e)) {
                    logger.error(() -> format("Error running %s", template), e);
                }
                throw new GeneralScriptException("Error running " + template, e);
            }
            return writer.toString();
        }

        public boolean shouldLogException(Throwable e) {
            return e.getCause() != null && e.getCause() instanceof MustacheInvalidParameterException == false;
        }
    }

}
