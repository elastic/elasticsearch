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

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ProcessorConditionalScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readMap;

public class ConditionalProcessor extends AbstractProcessor {

    static final String TYPE = "conditional";

    private final Script condition;

    private final ScriptService scriptService;

    private final Processor processor;

    private ConditionalProcessor(String tag, Script script, ScriptService scriptService, Processor processor) {
        super(tag);
        this.condition = script;
        this.scriptService = scriptService;
        this.processor = processor;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        if (scriptService.compile(condition, ProcessorConditionalScript.CONTEXT)
            .newInstance(condition.getParams()).execute(IngestDocument.deepCopyMap(ingestDocument.getSourceAndMetadata()))) {
            processor.execute(ingestDocument);
        }
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
        public ConditionalProcessor create(Map<String, Processor.Factory> factories, String tag,
            Map<String, Object> config) throws Exception {
            Map<String, Map<String, Object>> processorConfig = readMap(TYPE, tag, config, "processor");
            final Script script;
            try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)
                .map(normalizeScript(config.get("script")));
                 InputStream stream = BytesReference.bytes(builder).streamInput();
                 XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                     LoggingDeprecationHandler.INSTANCE, stream)) {
                script = Script.parse(parser);
                config.remove("script");
                // verify script is able to be compiled before successfully creating processor.
                try {
                    scriptService.compile(script, ProcessorConditionalScript.CONTEXT);
                } catch (ScriptException e) {
                    throw newConfigurationException(TYPE, tag, null, e);
                }
            }
            Map.Entry<String, Map<String, Object>> entry = processorConfig.entrySet().iterator().next();
            Processor processor = ConfigurationUtils.readProcessor(factories, entry.getKey(), entry.getValue());
            return new ConditionalProcessor(tag, script, scriptService, processor);
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> normalizeScript(Object scriptConfig) {
            if (scriptConfig instanceof Map<?, ?>) {
                return (Map<String, Object>) scriptConfig;
            } else if (scriptConfig instanceof String) {
                return Collections.singletonMap("source", scriptConfig);
            } else {
                throw newConfigurationException(TYPE, null, "script",
                    "property isn't a map or string, but of type [" + scriptConfig.getClass().getName() + "]");
            }
        }
    }
}
