/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.datageneration.FieldType;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class NumberFieldBlockLoaderTestCase<T extends Number> extends BlockLoaderTestCase {
    public NumberFieldBlockLoaderTestCase(FieldType fieldType, Params params) {
        super(fieldType.toString(), params);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        var nullValue = fieldMapping.get("null_value") != null ? convert((Number) fieldMapping.get("null_value"), fieldMapping) : null;

        if (value instanceof List<?> == false) {
            return convert(value, nullValue, fieldMapping);
        }

        boolean hasDocValues = hasDocValues(fieldMapping, true);
        boolean useDocValues = params.preference() == MappedFieldType.FieldExtractPreference.NONE
            || params.preference() == MappedFieldType.FieldExtractPreference.DOC_VALUES
            || params.syntheticSource();
        if (hasDocValues && useDocValues) {
            // Sorted
            var resultList = ((List<Object>) value).stream()
                .map(v -> convert(v, nullValue, fieldMapping))
                .filter(Objects::nonNull)
                .sorted()
                .toList();
            return maybeFoldList(resultList);
        }

        // parsing from source
        var resultList = ((List<Object>) value).stream().map(v -> convert(v, nullValue, fieldMapping)).filter(Objects::nonNull).toList();
        return maybeFoldList(resultList);
    }

    private T convert(Object value, T nullValue, Map<String, Object> fieldMapping) {
        switch (value) {
            case null -> {
                return nullValue;
            }

            // String coercion is true by default
            case String s -> {
                if (s.isEmpty()) {
                    return nullValue;
                }
                // Attempt to parse the string as a number. If that fails, the string is malformed, so return null
                Number parsed = tryParseString(s);
                if (parsed != null) {
                    return convert(parsed, fieldMapping);
                }
                return null;
            }

            case Number n -> {
                return convert(n, fieldMapping);
            }

            default -> {
                // Malformed values are excluded
                return null;
            }
        }
    }

    /**
     * Tries to parse a string as a number, matching the behavior of UnsignedLongFieldMapper.parseUnsignedLong().
     * Returns null if the string cannot be parsed as a valid number.
     */
    private Number tryParseString(String s) {
        try {
            return Long.parseUnsignedLong(s);
        } catch (NumberFormatException ex1) {
            try {
                BigDecimal bd = new BigDecimal(s);
                return bd.toBigIntegerExact();
            } catch (NumberFormatException | ArithmeticException ex2) {
                // not a valid number
                return null;
            }
        }
    }

    protected abstract T convert(Number value, Map<String, Object> fieldMapping);
}
