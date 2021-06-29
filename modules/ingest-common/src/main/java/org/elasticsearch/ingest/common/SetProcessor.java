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
import org.elasticsearch.ingest.ValueSource;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that adds new fields with their corresponding values. If the field is already present, its value
 * will be replaced with the provided one.
 */
public final class SetProcessor extends AbstractProcessor {

    public static final String TYPE = "set";

    private final boolean overrideEnabled;
    private final TemplateScript.Factory field;
    private final ValueSource value;
    private final String copyFrom;
    private final boolean ignoreEmptyValue;

    SetProcessor(String tag, String description, TemplateScript.Factory field, ValueSource value, String copyFrom) {
        this(tag, description, field, value, copyFrom, true, false);
    }

    SetProcessor(String tag, String description, TemplateScript.Factory field, ValueSource value, String copyFrom, boolean overrideEnabled,
                 boolean ignoreEmptyValue) {
        super(tag, description);
        this.overrideEnabled = overrideEnabled;
        this.field = field;
        this.value = value;
        this.copyFrom = copyFrom;
        this.ignoreEmptyValue = ignoreEmptyValue;
    }

    public boolean isOverrideEnabled() {
        return overrideEnabled;
    }

    public TemplateScript.Factory getField() {
        return field;
    }

    public ValueSource getValue() {
        return value;
    }

    public String getCopyFrom() {
        return copyFrom;
    }

    public boolean isIgnoreEmptyValue() {
        return ignoreEmptyValue;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        if (overrideEnabled || document.hasField(field) == false || document.getFieldValue(field, Object.class) == null) {
            if (copyFrom != null) {
                Object fieldValue = document.getFieldValue(copyFrom, Object.class, ignoreEmptyValue);
                document.setFieldValue(field, IngestDocument.deepCopy(fieldValue), ignoreEmptyValue);
            } else {
                document.setFieldValue(field, value, ignoreEmptyValue);
            }
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
        public SetProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                   String description, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String copyFrom = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "copy_from");
            String mediaType = ConfigurationUtils.readMediaTypeProperty(TYPE, processorTag, config, "media_type", "application/json");
            ValueSource valueSource = null;
            if (copyFrom == null) {
                Object value = ConfigurationUtils.readObject(TYPE, processorTag, config, "value");
                valueSource = ValueSource.wrap(value, scriptService, Map.of(Script.CONTENT_TYPE_OPTION, mediaType));
            } else {
                Object value = config.remove("value");
                if (value != null) {
                    throw newConfigurationException(TYPE, processorTag, "copy_from",
                        "cannot set both `copy_from` and `value` in the same processor");
                }
            }

            boolean overrideEnabled = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "override", true);
            TemplateScript.Factory compiledTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", field, scriptService);
            boolean ignoreEmptyValue = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_empty_value", false);

            return new SetProcessor(
                    processorTag,
                    description,
                    compiledTemplate,
                    valueSource,
                    copyFrom,
                    overrideEnabled,
                    ignoreEmptyValue);
        }
    }
}
