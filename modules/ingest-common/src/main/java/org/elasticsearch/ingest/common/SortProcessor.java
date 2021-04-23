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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Processor that sorts an array of items.
 * Throws exception is the specified field is not an array.
 */
public final class SortProcessor extends AbstractProcessor {

    public static final String TYPE = "sort";
    public static final String FIELD = "field";
    public static final String ORDER = "order";
    public static final String DEFAULT_ORDER = "asc";

    public enum SortOrder {
        ASCENDING("asc"), DESCENDING("desc");

        private final String direction;

        SortOrder(String direction) {
            this.direction = direction;
        }

        @Override
        public String toString() {
            return this.direction;
        }

        public static SortOrder fromString(String value) {
            if (value == null) {
                throw new IllegalArgumentException("Sort direction cannot be null");
            }

            if (value.equals(ASCENDING.toString())) {
                return ASCENDING;
            } else if (value.equals(DESCENDING.toString())) {
                return DESCENDING;
            }
            throw new IllegalArgumentException("Sort direction [" + value + "] not recognized."
                    + " Valid values are: [asc, desc]");
        }
    }

    private final String field;
    private final SortOrder order;
    private final String targetField;

    SortProcessor(String tag, String description, String field, SortOrder order, String targetField) {
        super(tag, description);
        this.field = field;
        this.order = order;
        this.targetField = targetField;
    }

    String getField() {
        return field;
    }

    SortOrder getOrder() {
        return order;
    }

    String getTargetField() {
        return targetField;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IngestDocument execute(IngestDocument document) {
        List<? extends Comparable<Object>> list = document.getFieldValue(field, List.class);

        if (list == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot sort.");
        }

        List<? extends Comparable<Object>> copy = new ArrayList<>(list);

        if (order.equals(SortOrder.ASCENDING)) {
            Collections.sort(copy);
        } else {
            Collections.sort(copy, Collections.reverseOrder());
        }

        document.setFieldValue(targetField, copy);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public SortProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                    String description, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, FIELD);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            try {
                SortOrder direction = SortOrder.fromString(
                    ConfigurationUtils.readStringProperty(
                        TYPE,
                        processorTag,
                        config,
                        ORDER,
                        DEFAULT_ORDER));
                return new SortProcessor(processorTag, description, field, direction, targetField);
            } catch (IllegalArgumentException e) {
                throw ConfigurationUtils.newConfigurationException(TYPE, processorTag, ORDER, e.getMessage());
            }
        }
    }
}

