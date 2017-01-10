/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler.extractor;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class SearchHitFieldExtractor {

    private SearchHitFieldExtractor() {}

    public static Object[] extractField(SearchHit hit, String field) {
        SearchHitField keyValue = hit.field(field);
        if (keyValue != null) {
            List<Object> values = keyValue.values();
            return values.toArray(new Object[values.size()]);
        } else {
            return extractFieldFromSource(hit.getSource(), field);
        }
    }

    private static Object[] extractFieldFromSource(Map<String, Object> source, String field) {
        if (source != null) {
            Object values = source.get(field);
            if (values != null) {
                if (values instanceof Object[]) {
                    return (Object[]) values;
                } else {
                    return new Object[]{values};
                }
            }
        }
        return new Object[0];
    }

    public static Long extractTimeField(SearchHit hit, String timeField) {
        Object[] fields = extractField(hit, timeField);
        if (fields.length != 1) {
            throw new RuntimeException("Time field [" + timeField + "] expected a single value; actual was: " + Arrays.toString(fields));
        }
        if (fields[0] instanceof Long) {
            return (Long) fields[0];
        }
        throw new RuntimeException("Time field [" + timeField + "] expected a long value; actual was: " + fields[0]);
    }
}
