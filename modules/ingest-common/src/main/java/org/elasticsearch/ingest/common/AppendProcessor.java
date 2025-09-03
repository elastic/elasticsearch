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
 * Processor that appends value or values to existing lists. If the field is not present a new list holding the
 * provided values will be added. If the field is a scalar it will be converted to a single item list and the provided
 * values will be added to the newly created list.
 */
public final class AppendProcessor extends AbstractProcessor {

    public static final String TYPE = "append";

    private final TemplateScript.Factory field;
    private final ValueSource value;
    private final String copyFrom;
    private final boolean allowDuplicates;

    AppendProcessor(
        String tag,
        String description,
        TemplateScript.Factory field,
        ValueSource value,
        String copyFrom,
        boolean allowDuplicates
    ) {
        super(tag, description);
        this.field = field;
        this.value = value;
        this.copyFrom = copyFrom;
        this.allowDuplicates = allowDuplicates;
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

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        String path = document.renderTemplate(field);
        if (copyFrom != null) {
            Object fieldValue = document.getFieldValue(copyFrom, Object.class);
            document.appendFieldValue(path, IngestDocument.deepCopy(fieldValue), allowDuplicates);
        } else {
            document.appendFieldValue(path, value, allowDuplicates);
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
        public AppendProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
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
                    throw newConfigurationException(
                        TYPE,
                        processorTag,
                        "copy_from",
                        "cannot set both `copy_from` and `value` in the same processor"
                    );
                }
            }
            boolean allowDuplicates = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "allow_duplicates", true);
            TemplateScript.Factory compiledTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", field, scriptService);

            return new AppendProcessor(processorTag, description, compiledTemplate, valueSource, copyFrom, allowDuplicates);
        }
    }
}
