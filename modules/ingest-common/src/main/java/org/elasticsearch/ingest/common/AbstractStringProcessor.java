/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class for processors that manipulate source strings and require a single "fields" array config value, which
 * holds a list of field names in string format.
 *
 * @param <T> The resultant type for the target field
 */
abstract class AbstractStringProcessor<T> extends AbstractProcessor {
    private final String field;
    private final boolean ignoreMissing;
    private final String targetField;

    AbstractStringProcessor(String tag, String description, boolean ignoreMissing, String targetField, String field) {
        super(tag, description);
        this.field = field;
        this.ignoreMissing = ignoreMissing;
        this.targetField = targetField;
    }

    public String getField() {
        return field;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    String getTargetField() {
        return targetField;
    }

    @Override
    public final IngestDocument execute(IngestDocument document) {
        Object val = document.getFieldValue(field, Object.class, ignoreMissing);
        Object newValue;

        if (val == null && ignoreMissing) {
            return document;
        } else if (val == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }

        if (val instanceof List<?> list) {
            List<Object> newList = new ArrayList<>(list.size());
            for (Object value : list) {
                if (value instanceof String string) {
                    newList.add(process(string));
                } else {
                    throw new IllegalArgumentException(
                        "value ["
                            + value
                            + "] of type ["
                            + value.getClass().getName()
                            + "] in list field ["
                            + field
                            + "] cannot be cast to ["
                            + String.class.getName()
                            + "]"
                    );
                }
            }
            newValue = newList;
        } else {
            if (val instanceof String string) {
                newValue = process(string);
            } else {
                throw new IllegalArgumentException(
                    "field [" + field + "] of type [" + val.getClass().getName() + "] cannot be cast to [" + String.class.getName() + "]"
                );
            }

        }

        document.setFieldValue(targetField, newValue);
        return document;
    }

    protected abstract T process(String value);

    abstract static class Factory implements Processor.Factory {
        final String processorType;

        protected Factory(String processorType) {
            this.processorType = processorType;
        }

        @Override
        public AbstractStringProcessor<?> create(
            Map<String, Processor.Factory> registry,
            String tag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(processorType, tag, config, "field");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(processorType, tag, config, "ignore_missing", false);
            String targetField = ConfigurationUtils.readStringProperty(processorType, tag, config, "target_field", field);

            return newProcessor(tag, description, config, field, ignoreMissing, targetField);
        }

        protected abstract AbstractStringProcessor<?> newProcessor(
            String processorTag,
            String description,
            Map<String, Object> config,
            String field,
            boolean ignoreMissing,
            String targetField
        );
    }
}
