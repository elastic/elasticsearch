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
import org.elasticsearch.ingest.common.CefParser.CefEvent;
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
    final boolean ignoreEmptyValues;
    private final TemplateScript.Factory timezone;

    CefProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean ignoreMissing,
        boolean ignoreEmptyValues,
        @Nullable TemplateScript.Factory timezone
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.ignoreEmptyValues = ignoreEmptyValues;
        this.timezone = timezone;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        String line = document.getFieldValue(field, String.class, ignoreMissing);
        if (line == null && ignoreMissing) {
            return document;
        } else if (line == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }
        ZoneId timezone = getTimezone(document);
        try (CefEvent event = new CefParser(timezone, ignoreEmptyValues).process(line)) {
            event.getRootMappings().forEach(document::setFieldValue);
            event.getCefMappings().forEach((k, v) -> document.setFieldValue(targetField + "." + k, v));
        }
        return document;
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

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public CefProcessor create(
            Map<String, Processor.Factory> registry,
            String tag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) {
            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "target_field", "cef");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            boolean ignoreEmptyValues = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_empty_values", true);
            String timezoneString = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "timezone");
            TemplateScript.Factory compiledTimezoneTemplate = null;
            if (timezoneString != null) {
                compiledTimezoneTemplate = ConfigurationUtils.compileTemplate(TYPE, tag, "timezone", timezoneString, scriptService);
            }

            return new CefProcessor(tag, description, field, targetField, ignoreMissing, ignoreEmptyValues, compiledTimezoneTemplate);
        }
    }
}
