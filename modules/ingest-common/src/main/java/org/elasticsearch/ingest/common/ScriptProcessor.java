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

import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalMap;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.script.ScriptType.FILE;
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
        ExecutableScript executableScript = scriptService.executable(script, ScriptContext.Standard.INGEST);
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

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ScriptProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                      Map<String, Object> config) throws Exception {
            String lang = readOptionalStringProperty(TYPE, processorTag, config, "lang");
            String inline = readOptionalStringProperty(TYPE, processorTag, config, "inline");
            String file = readOptionalStringProperty(TYPE, processorTag, config, "file");
            String id = readOptionalStringProperty(TYPE, processorTag, config, "id");
            Map<String, ?> params = readOptionalMap(TYPE, processorTag, config, "params");

            boolean containsNoScript = !hasLength(file) && !hasLength(id) && !hasLength(inline);
            if (containsNoScript) {
                throw newConfigurationException(TYPE, processorTag, null, "Need [file], [id], or [inline] parameter to refer to scripts");
            }

            boolean moreThanOneConfigured = (Strings.hasLength(file) && Strings.hasLength(id)) ||
                (Strings.hasLength(file) && Strings.hasLength(inline)) || (Strings.hasLength(id) && Strings.hasLength(inline));
            if (moreThanOneConfigured) {
                throw newConfigurationException(TYPE, processorTag, null, "Only one of [file], [id], or [inline] may be configured");
            }

            if (lang == null) {
                lang = Script.DEFAULT_SCRIPT_LANG;
            }

            if (params == null) {
                params = emptyMap();
            }

            final Script script;
            String scriptPropertyUsed;
            if (Strings.hasLength(file)) {
                script = new Script(FILE, lang, file, (Map<String, Object>)params);
                scriptPropertyUsed = "file";
            } else if (Strings.hasLength(inline)) {
                script = new Script(INLINE, lang, inline, (Map<String, Object>)params);
                scriptPropertyUsed = "inline";
            } else if (Strings.hasLength(id)) {
                script = new Script(STORED, lang, id, (Map<String, Object>)params);
                scriptPropertyUsed = "id";
            } else {
                throw newConfigurationException(TYPE, processorTag, null, "Could not initialize script");
            }

            // verify script is able to be compiled before successfully creating processor.
            try {
                scriptService.compile(script, ScriptContext.Standard.INGEST);
            } catch (ScriptException e) {
                throw newConfigurationException(TYPE, processorTag, scriptPropertyUsed, e);
            }

            return new ScriptProcessor(processorTag, script, scriptService);
        }
    }
}
