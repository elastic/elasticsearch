/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BooleanFieldBlockLoaderTests extends BlockLoaderTestCase {
    public BooleanFieldBlockLoaderTests(Params params) {
        super(FieldType.BOOLEAN.toString(), params);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        var nullValue = switch (fieldMapping.get("null_value")) {
            case Boolean b -> b;
            case String s -> Boolean.parseBoolean(s);
            case null -> null;
            default -> throw new IllegalStateException("Unexpected null_value format");
        };

        if (value instanceof List<?> == false) {
            return convert(value, nullValue);
        }

        if ((boolean) fieldMapping.getOrDefault("doc_values", false)) {
            // Sorted
            var resultList = ((List<Object>) value).stream().map(v -> convert(v, nullValue)).filter(Objects::nonNull).sorted().toList();
            return maybeFoldList(resultList);
        }

        // parsing from source, not sorted
        var resultList = ((List<Object>) value).stream().map(v -> convert(v, nullValue)).filter(Objects::nonNull).toList();
        return maybeFoldList(resultList);
    }

    @SuppressWarnings("unchecked")
    private Object convert(Object value, Object nullValue) {
        if (value == null) {
            return nullValue;
        }
        if (value instanceof String s) {
            if (s.isEmpty()) {
                // This is a documented behavior.
                return false;
            }
            if (value.equals("true")) {
                return true;
            }
            if (value.equals("false")) {
                return false;
            }
        }
        if (value instanceof Boolean b) {
            return b;
        }

        // Malformed values are excluded
        return null;
    }
}
