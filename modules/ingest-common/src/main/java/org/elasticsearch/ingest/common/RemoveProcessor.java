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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Processor that removes existing fields. Nothing happens if the field is not present.
 */
public final class RemoveProcessor extends AbstractProcessor {

    public static final String TYPE = "remove";

    private final List<TemplateScript.Factory> fields;

    RemoveProcessor(String tag, List<TemplateScript.Factory> fields) {
        super(tag);
        this.fields = new ArrayList<>(fields);
    }

    public List<TemplateScript.Factory> getFields() {
        return fields;
    }

    @Override
    public void execute(IngestDocument document) {
       fields.forEach(document::removeField);
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
        public RemoveProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                      Map<String, Object> config) throws Exception {
            final List<String> fields = new ArrayList<>();
            final Object field = ConfigurationUtils.readObject(TYPE, processorTag, config, "field");
            if (field instanceof List) {
                fields.addAll((List) field);
            } else {
                fields.add((String) field);
            }

            final List<TemplateScript.Factory> compiledTemplates = fields.stream()
                .map(f -> ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", f, scriptService))
                .collect(Collectors.toList());
            return new RemoveProcessor(processorTag, compiledTemplates);
        }
    }
}

