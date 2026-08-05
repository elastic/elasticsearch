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
            || params.syntheticSource()
            || params.isColumnarStored();

        ValueSource source;
        if (hasDocValues && useDocValues) {
            source = ValueSource.DOC_VALUES;
        } else if (params.syntheticSource()) {
            source = ValueSource.IGNORED_SOURCE;
        } else {
            source = ValueSource.STORED_SOURCE;
        }

        if (value instanceof List<?> == false) {
            return convert(value, nullValue, fieldMapping, source);
        }

        if (source == ValueSource.DOC_VALUES) {
            // Columnar index modes preserve arrival order via offsets; standard mode returns doc values sorted.
            boolean preserveOrder = params.indexMode().isColumnar();
            var stream = ((List<Object>) value).stream().map(v -> convert(v, nullValue, fieldMapping, source)).filter(Objects::nonNull);
            var resultList = preserveOrder ? stream.toList() : stream.sorted().toList();
            return maybeFoldList(resultList);
        }

        // parsing from source
        var resultList = ((List<Object>) value).stream()
            .map(v -> convert(v, nullValue, fieldMapping, source))
            .filter(Objects::nonNull)
            .toList();
        return maybeFoldList(resultList);
    }

    /**
     * Identifies which code path the block loader uses to retrieve a value. The distinction matters
     * because {@code NumberType.parse(XContentParser, boolean)} and
     * {@code NumberType.parse(Object, boolean)} can differ in how they handle non-latin Unicode digits.
     *
     * <p>{@link #DOC_VALUES} and {@link #IGNORED_SOURCE} both use the {@code XContentParser}
     * overload, so they share the same string-parsing behavior. {@link #STORED_SOURCE} uses the
     * {@code Object} overload and may therefore produce different results for the same input.
     */
    private enum ValueSource {
        /** Values indexed into doc values via the {@code XContentParser} overload. */
        DOC_VALUES,
        /**
         * Synthetic source without doc values: values stored in {@code _ignored_source} and
         * re-parsed by {@code NumberFallbackSyntheticSourceReader} using the {@code XContentParser}
         * overload.
         */
        IGNORED_SOURCE,
        /**
         * Non-synthetic source without doc values: values re-parsed from stored {@code _source} by
         * the {@code SourceValueFetcher} returned from {@code NumberFieldType#sourceValueFetcher},
         * using the {@code Object} overload.
         */
        STORED_SOURCE
    }

    private T convert(Object value, T nullValue, Map<String, Object> fieldMapping, ValueSource valueSource) {
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
                // The three block loader code paths use different parsers, so we delegate to the appropriate method.
                Number parsed = switch (valueSource) {
                    case DOC_VALUES, IGNORED_SOURCE -> tryParseString(s);
                    case STORED_SOURCE -> tryParseStringFromSource(s);
                };
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
     * Tries to parse a string as a number, matching the behavior used during indexing (doc values)
     * and when reading from {@code _ignored_source} via {@code NumberFallbackSyntheticSourceReader}.
     * Returns null if the string cannot be parsed as a valid number.
     *
     * <p>Both paths use {@code NumberType.parse(XContentParser, boolean)}. The default implementation
     * uses {@link Double#parseDouble(String)}, which matches the coercion behavior of most numeric
     * field mappers.
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
     * original stored {@code _source} (via the {@code SourceValueFetcher} returned from
     * {@code NumberFieldType#sourceValueFetcher}). Returns null if the string cannot be parsed as a
     * valid number.
     *
     * <p>The stored-source path uses {@code NumberType.parse(Object, boolean)}.
     *
     * <p>This is intentionally separate from {@link #tryParseString(String)} because the two paths can
     * differ (see {@code LongFieldBlockLoaderTests}).
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

    public void testBlockLoaderNonLatinDigit_parseFromSource() throws IOException {
        // This scenario relies on doc_values:false to force loading from source, which columnar modes don't allow.
        assumeFalse("doc_values cannot be disabled in columnar modes", params.indexMode().isStrictColumnar());
        runner.breaker(newLimitedBreaker(TEST_BREAKER_SIZE));

        String value = "\u1a90";

        runner.document(Map.of("field", value));
        runner.fieldName("field");

        var fieldMapping = new HashMap<String, Object>();
        fieldMapping.put("type", fieldType);
        fieldMapping.put("ignore_malformed", "true");
        fieldMapping.put("doc_values", false);
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
