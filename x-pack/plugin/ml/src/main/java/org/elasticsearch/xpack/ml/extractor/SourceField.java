/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SourceField extends AbstractField {

    private final String[] path;

    public SourceField(String name, Set<String> types) {
        super(name, types);
        path = name.split("\\.");
    }

    @Override
    public Method getMethod() {
        return Method.SOURCE;
    }

    @Override
    public Object[] value(SearchHit hit) {
        Map<String, Object> source = hit.getSourceAsMap();
        int level = 0;
        while (source != null && level < path.length - 1) {
            source = getNextLevel(source, path[level]);
            level++;
        }
        if (source != null) {
            Object values = source.get(path[level]);
            if (values != null) {
                if (values instanceof List<?>) {
                    @SuppressWarnings("unchecked")
                    List<Object> asList = (List<Object>) values;
                    return asList.toArray(new Object[0]);
                } else {
                    return new Object[] { values };
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
        if (nextLevel instanceof List<?> asList) {
            if (asList.isEmpty() == false) {
                Object firstElement = asList.get(0);
                if (firstElement instanceof Map<?, ?>) {
                    return (Map<String, Object>) firstElement;
                }
            }
        }
        return null;
    }

    @Override
    public boolean supportsFromSource() {
        return true;
    }

    @Override
    public ExtractedField newFromSource() {
        return this;
    }

    @Override
    public boolean isMultiField() {
        return false;
    }
}
