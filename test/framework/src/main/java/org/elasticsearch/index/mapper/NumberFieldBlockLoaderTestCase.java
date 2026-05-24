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
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
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

        boolean hasDocValues = hasDocValues(fieldMapping, true);
        boolean useDocValues = params.preference() == MappedFieldType.FieldExtractPreference.NONE
            || params.preference() == MappedFieldType.FieldExtractPreference.DOC_VALUES
            || params.syntheticSource();

        boolean fromDocValues = hasDocValues && useDocValues;

        if (value instanceof List<?> == false) {
            return convert(value, nullValue, fieldMapping, fromDocValues);
        }

        if (fromDocValues) {
            // Sorted
            var resultList = ((List<Object>) value).stream()
                .map(v -> convert(v, nullValue, fieldMapping, true))
                .filter(Objects::nonNull)
                .sorted()
                .toList();
            return maybeFoldList(resultList);
        }

        // parsing from source
        var resultList = ((List<Object>) value).stream()
            .map(v -> convert(v, nullValue, fieldMapping, false))
            .filter(Objects::nonNull)
            .toList();
        return maybeFoldList(resultList);
    }

    private T convert(Object value, T nullValue, Map<String, Object> fieldMapping, boolean fromDocValues) {
        switch (value) {
            case null -> {
                return nullValue;
            }

            // String coercion is true by default
            case String s -> {
                if (s.isEmpty()) {
                    return nullValue;
                }
                // Attempt to parse the string as a number. If that fails, the string is malformed, so return null.
                // The two code paths in the mapper use different parsers, so we delegate to the appropriate method.
                Number parsed = fromDocValues ? tryParseString(s) : tryParseStringFromSource(s);
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
     * Tries to parse a string as a number, matching the behavior used during indexing (and therefore
     * what is stored in doc values or returned via synthetic source).
     * Returns null if the string cannot be parsed as a valid number.
     *
     * <p>The default implementation uses {@link Double#parseDouble(String)}, which matches the
     * coercion behavior of most numeric field mappers.
     */
    protected Number tryParseString(String s) {
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    /**
     * Tries to parse a string as a number, matching the behavior used when re-parsing from the
     * original stored {@code _source}. Returns null if the string cannot be parsed as a valid number.
     *
     * <p>This is intentionally separate from {@link #tryParseString(String)} because the indexing
     * path and the stored-source re-parsing path can differ (see {@code LongFieldBlockLoaderTests}).
     */
    protected Number tryParseStringFromSource(String s) {
        return tryParseString(s);
    }

    protected abstract T convert(Number value, Map<String, Object> fieldMapping);

    public void testBlockLoaderNonLatinDigit() throws IOException {
        runner.breaker(newLimitedBreaker(TEST_BREAKER_SIZE));

        String value = "\u1a90";

        runner.document(Map.of("field", value));
        runner.fieldName("field");

        var fieldMapping = new HashMap<String, Object>();
        fieldMapping.put("type", fieldType);
        fieldMapping.put("ignore_malformed", "true");
        if (fieldType.equals(FieldType.SCALED_FLOAT.toString())) {
            fieldMapping.put("scaling_factor", "1");
        }

        var mapping = new Mapping(Map.of("_doc", Map.of("properties", Map.of("field", fieldMapping))), Map.of("field", fieldMapping));

        Object expected = expected(mapping.lookup().get("field"), value, new TestContext(false, false));

        var settings = getSettingsForParams();
        runner.mapperService(createMapperService(settings.build(), XContentFactory.jsonBuilder().map(mapping.raw())));
        runner.run(expected);
    }
}
