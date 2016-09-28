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

import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalMap;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.script.ScriptService.ScriptType.FILE;
import static org.elasticsearch.script.ScriptService.ScriptType.INLINE;
import static org.elasticsearch.script.ScriptService.ScriptType.STORED;

/**
 * Processor that adds new fields with their corresponding values. If the field is already present, its value
 * will be replaced with the provided one.
 */
public final class ScriptProcessor extends AbstractProcessor {

    public static final String TYPE = "script";

    private final Script script;
    private final ScriptService scriptService;

    ScriptProcessor(String tag, Script script, ScriptService scriptService)  {
        super(tag);
        this.script = script;
        this.scriptService = scriptService;
    }

    @Override
    public void execute(IngestDocument document) {
        ExecutableScript executableScript = scriptService.executable(script, ScriptContext.Standard.INGEST, emptyMap());
        executableScript.setNextVar("ctx",  document.getSourceAndMetadata());
        executableScript.run();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public ScriptProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                      Map<String, Object> config) throws Exception {
            String lang = readStringProperty(TYPE, processorTag, config, "lang");
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

            if(params == null) {
                params = emptyMap();
            }

            final Script script;
            if (Strings.hasLength(file)) {
                script = new Script(file, FILE, lang, params);
            } else if (Strings.hasLength(inline)) {
                script = new Script(inline, INLINE, lang, params);
            } else if (Strings.hasLength(id)) {
                script = new Script(id, STORED, lang, params);
            } else {
                throw newConfigurationException(TYPE, processorTag, null, "Could not initialize script");
            }

            return new ScriptProcessor(processorTag, script, scriptService);
        }
    }
}
