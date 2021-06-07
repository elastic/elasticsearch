/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    private final boolean ignoreMissing;

    RemoveProcessor(String tag, String description, List<TemplateScript.Factory> fields, boolean ignoreMissing) {
        super(tag, description);
        this.fields = new ArrayList<>(fields);
        this.ignoreMissing = ignoreMissing;
    }

    public List<TemplateScript.Factory> getFields() {
        return fields;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        if (ignoreMissing) {
            fields.forEach(field -> {
                String path = document.renderTemplate(field);
                if (document.hasField(path)) {
                    document.removeField(path);
                }
            });
        } else {
            fields.forEach(document::removeField);
        }
        return document;
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
                                      String description, Map<String, Object> config) throws Exception {
            final List<String> fields = new ArrayList<>();
            final Object field = ConfigurationUtils.readObject(TYPE, processorTag, config, "field");
            if (field instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> stringList = (List<String>) field;
                fields.addAll(stringList);
            } else {
                fields.add((String) field);
            }

            final List<TemplateScript.Factory> compiledTemplates = fields.stream()
                .map(f -> ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", f, scriptService))
                .collect(Collectors.toList());
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new RemoveProcessor(processorTag, description, compiledTemplates, ignoreMissing);
        }
    }
}

