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

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.util.Map;

/**
 * Processor that raises a runtime exception with a provided
 * error message.
 */
public final class FailProcessor extends AbstractProcessor {

    public static final String TYPE = "fail";

    private final TemplateScript.Factory message;

    FailProcessor(String tag, String description, TemplateScript.Factory message) {
        super(tag, description);
        this.message = message;
    }

    public TemplateScript.Factory getMessage() {
        return message;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        throw new FailProcessorException(document.renderTemplate(message));
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
        public FailProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                    String description, Map<String, Object> config) throws Exception {
            String message = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "message");
            TemplateScript.Factory compiledTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag,
                "message", message, scriptService);
            return new FailProcessor(processorTag, description, compiledTemplate);
        }
    }
}

