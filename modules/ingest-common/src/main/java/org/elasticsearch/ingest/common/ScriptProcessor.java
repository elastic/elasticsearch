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

package org.elasticsearch.ingest.common;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalMap;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.script.ScriptType.INLINE;
import static org.elasticsearch.script.ScriptType.STORED;

/**
 * Processor that evaluates a script with an ingest document in its context.
 */
public final class ScriptProcessor extends AbstractProcessor {

    public static final String TYPE = "script";

    private final Script script;
    private final ScriptService scriptService;

    /**
     * Processor that evaluates a script with an ingest document in its context
     *
     * @param tag The processor's tag.
     * @param script The {@link Script} to execute.
     * @param scriptService The {@link ScriptService} used to execute the script.
     */
    ScriptProcessor(String tag, Script script, ScriptService scriptService)  {
        super(tag);
        this.script = script;
        this.scriptService = scriptService;
    }

    /**
     * Executes the script with the Ingest document in context.
     *
     * @param document The Ingest document passed into the script context under the "ctx" object.
     */
    @Override
    public void execute(IngestDocument document) {
        ExecutableScript.Factory factory = scriptService.compile(script, ExecutableScript.INGEST_CONTEXT);
        ExecutableScript executableScript = factory.newInstance(script.getParams());
        executableScript.setNextVar("ctx",  document.getSourceAndMetadata());
        executableScript.run();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    Script getScript() {
        return script;
    }

    public static final class Factory implements Processor.Factory {
        private final Logger logger = ESLoggerFactory.getLogger(Factory.class);
        private final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ScriptProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                      Map<String, Object> config) throws Exception {
            String lang = readOptionalStringProperty(TYPE, processorTag, config, "lang");
            String source = readOptionalStringProperty(TYPE, processorTag, config, "source");
            String id = readOptionalStringProperty(TYPE, processorTag, config, "id");
            Map<String, ?> params = readOptionalMap(TYPE, processorTag, config, "params");

            if (source == null) {
                source = readOptionalStringProperty(TYPE, processorTag, config, "inline");
                if (source != null) {
                    deprecationLogger.deprecated("Specifying script source with [inline] is deprecated, use [source] instead.");
                }
            }

            boolean containsNoScript = !hasLength(id) && !hasLength(source);
            if (containsNoScript) {
                throw newConfigurationException(TYPE, processorTag, null, "Need [id] or [source] parameter to refer to scripts");
            }

            boolean moreThanOneConfigured = Strings.hasLength(id) && Strings.hasLength(source);
            if (moreThanOneConfigured) {
                throw newConfigurationException(TYPE, processorTag, null, "Only one of [id] or [source] may be configured");
            }

            if (lang == null) {
                lang = Script.DEFAULT_SCRIPT_LANG;
            }

            if (params == null) {
                params = emptyMap();
            }

            final Script script;
            String scriptPropertyUsed;
            if (Strings.hasLength(source)) {
                script = new Script(INLINE, lang, source, (Map<String, Object>)params);
                scriptPropertyUsed = "source";
            } else if (Strings.hasLength(id)) {
                script = new Script(STORED, null, id, (Map<String, Object>)params);
                scriptPropertyUsed = "id";
            } else {
                throw newConfigurationException(TYPE, processorTag, null, "Could not initialize script");
            }

            // verify script is able to be compiled before successfully creating processor.
            try {
                scriptService.compile(script, ExecutableScript.INGEST_CONTEXT);
            } catch (ScriptException e) {
                throw newConfigurationException(TYPE, processorTag, scriptPropertyUsed, e);
            }

            return new ScriptProcessor(processorTag, script, scriptService);
        }
    }
}
