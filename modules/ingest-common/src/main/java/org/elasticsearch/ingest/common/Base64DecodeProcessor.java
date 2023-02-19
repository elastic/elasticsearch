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

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Processor that decodes fields content in base64 format. This processor supports both padded and non-padded variants of the base64 string.
 * Throws exception if the field is not present or the decoding error.
 */
public final class Base64DecodeProcessor extends AbstractProcessor {

    public static final String TYPE = "base64decode";

    private final String field;
    private final String targetField;
    private final boolean ignoreMissing;
    private final boolean ignoreFailure;

    Base64DecodeProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean ignoreFailure,
        boolean ignoreMissing) {
            super(tag, description);
            this.field = field;
            this.targetField = targetField;
            this.ignoreMissing = ignoreMissing;
            this.ignoreFailure = ignoreFailure;
    }

    public String getField() {
        return field;
    }

    public String getTargetField() {
        return targetField;
    }

    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }

    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        Object originalValue = document.getFieldValue(field, Object.class, ignoreMissing);
        Object newValue;

        if (originalValue == null && ignoreMissing) {
            return document;
        } else if (originalValue == null) {
            throw new IllegalArgumentException("Field [" + field + "] is null, cannot decode the field");
        }

        if (originalValue instanceof List<?> list) {
            List<Object> newList = new ArrayList<>(list.size());
            for (Object value : list) {
                if (value instanceof String == false && ignoreFailure) {
                    continue;
                } else if (value instanceof String == false ){
                    throw new IllegalArgumentException("Cannot read non string value [ " + value + " ] of field [" + field + "]");
                }
                newList.add(decode(value.toString()));
            }
            newValue = newList;
        } else if (originalValue instanceof String) {
            newValue = decode(originalValue.toString());
        } else {
            throw new IllegalArgumentException(
                "Field [" + field + "] cannot be processed due to invalid value type: " + originalValue.getClass().getName()
            );
        }
        document.setFieldValue(targetField, newValue);
        return document;
    }

    private String decode(String encoded) {
        return new String(Base64.getDecoder().decode(encoded));
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public Base64DecodeProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean ignoreFailure = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_failure", false);
            return new Base64DecodeProcessor(processorTag, description, field, targetField, ignoreMissing, ignoreFailure);
        }
    }
}
