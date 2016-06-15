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

package org.elasticsearch.ingest;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.ingest.core.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readStringProperty;
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
    private final ClusterService clusterService;
    private final String field;

    ScriptProcessor(String tag, Script script, ScriptService scriptService, ClusterService clusterService, String field)  {
        super(tag);
        this.script = script;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.field = field;
    }

    @Override
    public void execute(IngestDocument document) {
        Map<String, Object> vars = new HashMap<>();
        vars.put("ctx", document.getSourceAndMetadata());
        CompiledScript compiledScript = scriptService.compile(script, ScriptContext.Standard.INGEST, emptyMap(), clusterService.state());
        ExecutableScript executableScript = scriptService.executable(compiledScript, vars);
        Object value = executableScript.run();
        if (field != null) {
            document.setFieldValue(field, value);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractProcessorFactory<ScriptProcessor> {

        private final ScriptService scriptService;
        private final ClusterService clusterService;

        public Factory(ScriptService scriptService, ClusterService clusterService) {
            this.scriptService = scriptService;
            this.clusterService = clusterService;
        }

        @Override
        public ScriptProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = readOptionalStringProperty(TYPE, processorTag, config, "field");
            String lang = readStringProperty(TYPE, processorTag, config, "lang");
            String inline = readOptionalStringProperty(TYPE, processorTag, config, "inline");
            String file = readOptionalStringProperty(TYPE, processorTag, config, "file");
            String id = readOptionalStringProperty(TYPE, processorTag, config, "id");

            boolean containsNoScript = !hasLength(file) && !hasLength(id) && !hasLength(inline);
            if (containsNoScript) {
                throw newConfigurationException(TYPE, processorTag, null, "Need [file], [id], or [inline] parameter to refer to scripts");
            }

            boolean moreThanOneConfigured = (Strings.hasLength(file) && Strings.hasLength(id)) ||
                (Strings.hasLength(file) && Strings.hasLength(inline)) || (Strings.hasLength(id) && Strings.hasLength(inline));
            if (moreThanOneConfigured) {
                throw newConfigurationException(TYPE, processorTag, null, "Only one of [file], [id], or [inline] may be configured");
            }

            final Script script;
            if (Strings.hasLength(file)) {
                script = new Script(file, FILE, lang, emptyMap());
            } else if (Strings.hasLength(inline)) {
                script = new Script(inline, INLINE, lang, emptyMap());
            } else if (Strings.hasLength(id)) {
                script = new Script(id, STORED, lang, emptyMap());
            } else {
                throw newConfigurationException(TYPE, processorTag, null, "Could not initialize script");
            }

            return new ScriptProcessor(processorTag, script, scriptService, clusterService, field);
        }
    }
}
