/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.xpack.sql.execution.ExecutionException;

class InnerHitExtractor implements HitExtractor {
    private final String hitName, fieldName;
    private final boolean useDocValue;
    private final String[] tree;

    InnerHitExtractor(String hitName, String name, boolean useDocValue) {
        this.hitName = hitName;
        this.fieldName = name;
        this.useDocValue = useDocValue;
        this.tree = useDocValue ? Strings.EMPTY_ARRAY : Strings.tokenizeToStringArray(name, ".");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object get(SearchHit hit) {
        if (useDocValue) {
            SearchHitField field = hit.field(fieldName);
            return field != null ? field.getValue() : null;
        }
        else {
            Map<String, Object> source = hit.getSourceAsMap();
            if (source == null) {
                return null;
            }
            Object value = null;
            for (String node : tree) {
                if (value != null) {
                    if (value instanceof Map) {
                        source = (Map<String, Object>) value;
                    }
                    else {
                        throw new ExecutionException("Cannot extract value %s from source", fieldName);
                    }
                }
                value = source.get(node);
            }
            return value;
        }
    }

    public String parent() {
        return hitName;
    }

    @Override
    public String toString() {
        return fieldName + "@" + hitName;
    }
}