/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;

public final class CefProcessor extends AbstractProcessor {

    public static final String TYPE = "cef";

    // visible for testing
    final String field;
    final String targetField;
    final boolean ignoreMissing;
    final boolean removeEmptyValue;
    private final TemplateScript.Factory timezone;

    CefProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean ignoreMissing,
        boolean removeEmptyValue,
        @Nullable TemplateScript.Factory timezone
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.removeEmptyValue = removeEmptyValue;
        this.timezone = timezone;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        String line = ingestDocument.getFieldValue(field, String.class, ignoreMissing);
        if (line == null && ignoreMissing) {
            return ingestDocument;
        } else if (line == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }
        ZoneId timezone = getTimezone(ingestDocument);
        CefParser.CEFEvent cefEvent = new CefParser(timezone, removeEmptyValue).process(line);
        // Update ingestDocument with the CEF mappings
        ingestDocument.setFieldValue(targetField, cefEvent.getCefMappings());
        cefEvent.getRootMappings().forEach(ingestDocument::setFieldValue);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    ZoneId getTimezone(IngestDocument document) {
        String value = timezone == null ? null : document.renderTemplate(timezone);
        if (value == null) {
            return ZoneOffset.UTC;
        } else {
            return ZoneId.of(value);
        }
    }

    public static final class Factory implements org.elasticsearch.ingest.Processor.Factory {
        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public CefProcessor create(
            Map<String, Processor.Factory> processorFactories,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            boolean removeEmptyValue = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_empty_value", true);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", "cef");
            String timezoneString = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "timezone");
            TemplateScript.Factory compiledTimezoneTemplate = null;
            if (timezoneString != null) {
                compiledTimezoneTemplate = ConfigurationUtils.compileTemplate(
                    TYPE,
                    processorTag,
                    "timezone",
                    timezoneString,
                    scriptService
                );
            }

            return new CefProcessor(
                processorTag,
                description,
                field,
                targetField,
                ignoreMissing,
                removeEmptyValue,
                compiledTimezoneTemplate
            );
        }
    }
}
