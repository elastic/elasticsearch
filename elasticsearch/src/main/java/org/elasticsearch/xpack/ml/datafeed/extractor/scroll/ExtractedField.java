/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.joda.time.base.BaseDateTime;

import java.util.List;
import java.util.Map;
import java.util.Objects;

abstract class ExtractedField {

    public enum ExtractionMethod {
        SOURCE, DOC_VALUE, SCRIPT_FIELD
    }

    protected final String name;
    private final ExtractionMethod extractionMethod;

    protected ExtractedField(String name, ExtractionMethod extractionMethod) {
        this.name = Objects.requireNonNull(name);
        this.extractionMethod = Objects.requireNonNull(extractionMethod);
    }

    public String getName() {
        return name;
    }

    public ExtractionMethod getExtractionMethod() {
        return extractionMethod;
    }

    public abstract Object[] value(SearchHit hit);

    public static ExtractedField newTimeField(String name, ExtractionMethod extractionMethod) {
        if (extractionMethod == ExtractionMethod.SOURCE) {
            throw new IllegalArgumentException("time field cannot be extracted from source");
        }
        return new TimeField(name, extractionMethod);
    }

    public static ExtractedField newField(String name, ExtractionMethod extractionMethod) {
        switch (extractionMethod) {
            case DOC_VALUE:
            case SCRIPT_FIELD:
                return new FromFields(name, extractionMethod);
            case SOURCE:
                return new FromSource(name, extractionMethod);
            default:
                throw new IllegalArgumentException("Invalid extraction method [" + extractionMethod + "]");
        }
    }

    private static class FromFields extends ExtractedField {

        FromFields(String name, ExtractionMethod extractionMethod) {
            super(name, extractionMethod);
        }

        @Override
        public Object[] value(SearchHit hit) {
            SearchHitField keyValue = hit.field(name);
            if (keyValue != null) {
                List<Object> values = keyValue.values();
                return values.toArray(new Object[values.size()]);
            }
            return new Object[0];
        }
    }

    private static class TimeField extends FromFields {

        TimeField(String name, ExtractionMethod extractionMethod) {
            super(name, extractionMethod);
        }

        @Override
        public Object[] value(SearchHit hit) {
            Object[] value = super.value(hit);
            if (value.length != 1) {
                return value;
            }
            value[0] = ((BaseDateTime) value[0]).getMillis();
            return value;
        }
    }

    private static class FromSource extends ExtractedField {

        private String[] namePath;

        FromSource(String name, ExtractionMethod extractionMethod) {
            super(name, extractionMethod);
            namePath = name.split("\\.");
        }

        @Override
        public Object[] value(SearchHit hit) {
            Map<String, Object> source = hit.getSource();
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
