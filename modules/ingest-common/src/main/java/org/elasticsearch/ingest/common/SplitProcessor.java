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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Processor that splits fields content into different items based on the occurrence of a specified separator.
 * New field value will be an array containing all of the different extracted items.
 * Support fields of string type only, throws exception if a field is of a different type.
 */
public final class SplitProcessor extends AbstractProcessor {

    public static final String TYPE = "split";

    private final String field;
    private final String separator;
    private final boolean ignoreMissing;
    private final boolean preserveTrailing;
    private final String targetField;

    SplitProcessor(
        String tag,
        String description,
        String field,
        String separator,
        boolean ignoreMissing,
        boolean preserveTrailing,
        String targetField
    ) {
        super(tag, description);
        this.field = field;
        this.separator = separator;
        this.ignoreMissing = ignoreMissing;
        this.preserveTrailing = preserveTrailing;
        this.targetField = targetField;
    }

    String getField() {
        return field;
    }

    String getSeparator() {
        return separator;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    boolean isPreserveTrailing() {
        return preserveTrailing;
    }

    String getTargetField() {
        return targetField;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        String oldVal = document.getFieldValue(field, String.class, ignoreMissing);

        if (oldVal == null && ignoreMissing) {
            return document;
        } else if (oldVal == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot split.");
        }

        String[] strings = oldVal.split(separator, preserveTrailing ? -1 : 0);
        List<String> splitList = new ArrayList<>(strings.length);
        Collections.addAll(splitList, strings);
        document.setFieldValue(targetField, splitList);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {
        @Override
        public SplitProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean preserveTrailing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "preserve_trailing", false);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            String separator = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "separator");
            return new SplitProcessor(processorTag, description, field, separator, ignoreMissing, preserveTrailing, targetField);
        }
    }
}
