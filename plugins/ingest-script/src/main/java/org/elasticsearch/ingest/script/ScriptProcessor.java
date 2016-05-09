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

package org.elasticsearch.ingest.script;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.script.ScriptService.ScriptType.FILE;
import static org.elasticsearch.script.ScriptService.ScriptType.INLINE;
import static org.elasticsearch.script.ScriptService.ScriptType.STORED;

public class ScriptProcessor extends AbstractProcessor {

    public static final String TYPE = "script";
    private final String field;
    private final Script script;
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    public ScriptProcessor(String tag, String field, Script script, ScriptService scriptService,
                           ClusterService clusterService) {
        super(tag);
        this.field = field;
        this.script = script;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> context = new HashMap<>();
        data.put("ctx", context);
        context.put("_source", ingestDocument.getSourceAndMetadata());
        CompiledScript compiledScript = scriptService.compile(script, ScriptContext.Standard.INGEST, emptyMap(), clusterService.state());
        Object returnValue = scriptService.executable(compiledScript, data).run();
        ingestDocument.setFieldValue(field, returnValue);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractProcessorFactory<ScriptProcessor> {

        private Node node;

        public Factory(Node node) {
            this.node = node;
        }

        @Override
        protected ScriptProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String lang = readOptionalStringProperty(TYPE, processorTag, config, "lang");
            String id = readOptionalStringProperty(TYPE, processorTag, config, "id");
            String file = readOptionalStringProperty(TYPE, processorTag, config, "file");
            String inline = readOptionalStringProperty(TYPE, processorTag, config, "inline");

            boolean containsNoScript = !Strings.hasLength(file) && !Strings.hasLength(id) && !Strings.hasLength(inline);
            if (containsNoScript) {
                throw new ElasticsearchException("Need [file], [id] or [inline] parameter to refer to scripts");
            }

            boolean moreThanOneConfigured = (Strings.hasLength(file) && Strings.hasLength(id)) ||
                                            (Strings.hasLength(file) && Strings.hasLength(inline)) ||
                                            (Strings.hasLength(id) && Strings.hasLength(inline));
            if (moreThanOneConfigured) {
                throw new ElasticsearchException("Only one of [file], [id] or [inline] may be configured");
            }

            final Script script;
            if (Strings.hasLength(file)) {
                script = new Script(file, FILE, lang, emptyMap());
            } else if (Strings.hasLength(inline)) {
                script = new Script(inline, INLINE, lang, emptyMap());
            } else if (Strings.hasLength(id)) {
                script = new Script(id, STORED, lang, emptyMap());
            } else {
                throw new ElasticsearchException("Could not initialize script");
            }

            ScriptService scriptService = node.injector().getInstance(ScriptService.class);
            ClusterService clusterService = node.injector().getInstance(ClusterService.class);
            return new ScriptProcessor(processorTag, field, script, scriptService, clusterService);
        }
    }
}
