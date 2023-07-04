/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that evaluates a script with an ingest document in its context.
 */
public final class ScriptProcessor extends AbstractProcessor {

    public static final String TYPE = "script";

    private final Script script;
    private final ScriptService scriptService;
    private final IngestScript.Factory precompiledIngestScriptFactory;

    /**
     * Processor that evaluates a script with an ingest document in its context
     *  @param tag The processor's tag.
     * @param description The processor's description.
     * @param script The {@link Script} to execute.
     * @param precompiledIngestScriptFactory The {@link Script} precompiled script
     * @param scriptService The {@link ScriptService} used to execute the script.
     */
    ScriptProcessor(
        String tag,
        String description,
        Script script,
        @Nullable IngestScript.Factory precompiledIngestScriptFactory,
        ScriptService scriptService
    ) {
        super(tag, description);
        this.script = script;
        this.precompiledIngestScriptFactory = precompiledIngestScriptFactory;
        this.scriptService = scriptService;
    }

    /**
     * Executes the script with the Ingest document in context.
     *
     * @param document The Ingest document passed into the script context under the "ctx" object.
     */
    @Override
    public IngestDocument execute(IngestDocument document) {
        document.doNoSelfReferencesCheck(true);
        IngestScript.Factory factory = precompiledIngestScriptFactory;
        if (factory == null) {
            factory = scriptService.compile(script, IngestScript.CONTEXT);
        }
        factory.newInstance(script.getParams(), document.getCtxMap()).execute();
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    Script getScript() {
        return script;
    }

    IngestScript.Factory getPrecompiledIngestScriptFactory() {
        return precompiledIngestScriptFactory;
    }

    public static final class Factory implements Processor.Factory {
        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public ScriptProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            try (
                XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(config);
                InputStream stream = BytesReference.bytes(builder).streamInput();
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE), stream)
            ) {
                Script script = Script.parse(parser);

                Arrays.asList("id", "source", "inline", "lang", "params", "options").forEach(config::remove);

                // verify script is able to be compiled before successfully creating processor.
                IngestScript.Factory ingestScriptFactory = null;
                try {
                    ingestScriptFactory = scriptService.compile(script, IngestScript.CONTEXT);
                    if (ScriptType.STORED.equals(script.getType())) {
                        // do not cache stored scripts lest they change and invalidate the cached value
                        ingestScriptFactory = null;
                    }
                } catch (ScriptException e) {
                    throw newConfigurationException(TYPE, processorTag, null, e);
                }
                return new ScriptProcessor(processorTag, description, script, ingestScriptFactory, scriptService);
            }
        }
    }
}
