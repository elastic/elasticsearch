/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.fields;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.joda.time.base.BaseDateTime;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a field to be extracted by the datafeed.
 * It encapsulates the extraction logic.
 */
public abstract class ExtractedField {

    public enum ExtractionMethod {
        SOURCE, DOC_VALUE, SCRIPT_FIELD
    }

    /** The name of the field as configured in the job */
    protected final String alias;

    /** The name of the field we extract */
    protected final String name;

    private final ExtractionMethod extractionMethod;

    protected ExtractedField(String alias, String name, ExtractionMethod extractionMethod) {
        this.alias = Objects.requireNonNull(alias);
        this.name = Objects.requireNonNull(name);
        this.extractionMethod = Objects.requireNonNull(extractionMethod);
    }

    public String getAlias() {
        return alias;
    }

    public String getName() {
        return name;
    }

    public ExtractionMethod getExtractionMethod() {
        return extractionMethod;
    }

    public abstract Object[] value(SearchHit hit);

    public String getDocValueFormat() {
        return DocValueFieldsContext.USE_DEFAULT_FORMAT;
    }

    public static ExtractedField newTimeField(String name, ExtractionMethod extractionMethod) {
        if (extractionMethod == ExtractionMethod.SOURCE) {
            throw new IllegalArgumentException("time field cannot be extracted from source");
        }
        return new TimeField(name, extractionMethod);
    }

    public static ExtractedField newField(String name, ExtractionMethod extractionMethod) {
        return newField(name, name, extractionMethod);
    }

    public static ExtractedField newField(String alias, String name, ExtractionMethod extractionMethod) {
        switch (extractionMethod) {
            case DOC_VALUE:
            case SCRIPT_FIELD:
                return new FromFields(alias, name, extractionMethod);
            case SOURCE:
                return new FromSource(alias, name, extractionMethod);
            default:
                throw new IllegalArgumentException("Invalid extraction method [" + extractionMethod + "]");
        }
    }

    private static class FromFields extends ExtractedField {

        FromFields(String alias, String name, ExtractionMethod extractionMethod) {
            super(alias, name, extractionMethod);
        }

        @Override
        public Object[] value(SearchHit hit) {
            DocumentField keyValue = hit.field(name);
            if (keyValue != null) {
                List<Object> values = keyValue.getValues();
                return values.toArray(new Object[values.size()]);
            }
            return new Object[0];
        }
    }

    private static class TimeField extends FromFields {

        private static final String EPOCH_MILLIS_FORMAT = "epoch_millis";

        TimeField(String name, ExtractionMethod extractionMethod) {
            super(name, name, extractionMethod);
        }

        @Override
        public Object[] value(SearchHit hit) {
            Object[] value = super.value(hit);
            if (value.length != 1) {
                return value;
            }
            if (value[0] instanceof String) { // doc_value field with the epoch_millis format
                value[0] = Long.parseLong((String) value[0]);
            } else if (value[0] instanceof BaseDateTime) { // script field
                value[0] = ((BaseDateTime) value[0]).getMillis();
            } else if (value[0] instanceof Long == false) { // pre-6.0 field
                throw new IllegalStateException("Unexpected value for a time field: " + value[0].getClass());
            }
            return value;
        }

        @Override
        public String getDocValueFormat() {
            return EPOCH_MILLIS_FORMAT;
        }
    }

    private static class FromSource extends ExtractedField {

        private String[] namePath;

        FromSource(String alias, String name, ExtractionMethod extractionMethod) {
            super(alias, name, extractionMethod);
            namePath = name.split("\\.");
        }

        @Override
        public Object[] value(SearchHit hit) {
            Map<String, Object> source = hit.getSourceAsMap();
            int level = 0;
            while (source != null && level < namePath.length - 1) {
                source = getNextLevel(source, namePath[level]);
                level++;
            }
            if (source != null) {
                Object values = source.get(namePath[level]);
                if (values != null) {
                    if (values instanceof List<?>) {
                        @SuppressWarnings("unchecked")
                        List<Object> asList = (List<Object>) values;
                        return asList.toArray(new Object[asList.size()]);
                    } else {
                        return new Object[]{values};
                    }
                }
            }
            return new Object[0];
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> getNextLevel(Map<String, Object> source, String key) {
            Object nextLevel = source.get(key);
            if (nextLevel instanceof Map<?, ?>) {
                return (Map<String, Object>) source.get(key);
            }
            return null;
        }
    }
}
