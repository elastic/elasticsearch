/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.logsdb.datageneration.FieldType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class NumberFieldBlockLoaderTestCase<T extends Number> extends BlockLoaderTestCase {
    public NumberFieldBlockLoaderTestCase(FieldType fieldType) {
        super(fieldType);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, boolean syntheticSource) {
        var nullValue = fieldMapping.get("null_value") != null ? convert((Number) fieldMapping.get("null_value")) : null;

        if (value instanceof List<?> == false) {
            return convert(value, nullValue);
        }

        if ((boolean) fieldMapping.getOrDefault("doc_values", false)) {
            // Sorted and no duplicates
            var resultList = ((List<Object>) value).stream().map(v -> convert(v, nullValue)).filter(Objects::nonNull).sorted().toList();
            return maybeFoldList(resultList);
        }

        // parsing from source
        var resultList = ((List<Object>) value).stream().map(v -> convert(v, nullValue)).filter(Objects::nonNull).toList();
        return maybeFoldList(resultList);
    }

    @SuppressWarnings("unchecked")
    private T convert(Object value, T nullValue) {
        if (value == null) {
            return nullValue;
        }
        // String coercion is true by default
        if (value instanceof String s && s.isEmpty()) {
            return nullValue;
        }
        if (value instanceof Number n) {
            return convert(n);
        }

        // Malformed values are excluded
        return null;
    }

    protected abstract T convert(Number value);
}
