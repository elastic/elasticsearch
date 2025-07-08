/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.matchers.source;

import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.matchers.MatchResult;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.datageneration.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.datageneration.matchers.Messages.prettyPrintCollections;

interface FieldSpecificMatcher {
    MatchResult match(List<Object> actual, List<Object> expected, Map<String, Object> actualMapping, Map<String, Object> expectedMapping);

    static Map<String, FieldSpecificMatcher> matchers(
        XContentBuilder actualMappings,
        Settings.Builder actualSettings,
        XContentBuilder expectedMappings,
        Settings.Builder expectedSettings
    ) {
        return new HashMap<>() {
            {
                put("keyword", new KeywordMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("long", new NumberMatcher("long", actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("unsigned_long", new UnsignedLongMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("integer", new NumberMatcher("integer", actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("short", new NumberMatcher("short", actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("byte", new NumberMatcher("byte", actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("double", new NumberMatcher("double", actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("float", new NumberMatcher("float", actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("half_float", new HalfFloatMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("scaled_float", new ScaledFloatMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("counted_keyword", new CountedKeywordMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("boolean", new BooleanMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("date", new DateMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("geo_shape", new ExactMatcher("geo_shape", actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("shape", new ExactMatcher("shape", actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("geo_point", new GeoPointMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("text", new TextMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("ip", new IpMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("constant_keyword", new ConstantKeywordMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
                put("wildcard", new WildcardMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings));
            }
        };
    }

    class CountedKeywordMatcher implements FieldSpecificMatcher {
        private final XContentBuilder actualMappings;
        private final Settings.Builder actualSettings;
        private final XContentBuilder expectedMappings;
        private final Settings.Builder expectedSettings;

        CountedKeywordMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            this.actualMappings = actualMappings;
            this.actualSettings = actualSettings;
            this.expectedMappings = expectedMappings;
            this.expectedSettings = expectedSettings;
        }

        @Override
        public MatchResult match(
            List<Object> actual,
            List<Object> expected,
            Map<String, Object> actualMapping,
            Map<String, Object> expectedMapping
        ) {
            var actualNormalized = normalize(actual);
            var expectedNormalized = normalize(expected);

            Map<String, Integer> counts = new TreeMap<>();
            for (String value : actualNormalized) {
                counts.put(value, counts.getOrDefault(value, 0) + 1);
            }
            for (String value : expectedNormalized) {
                int newCount = counts.getOrDefault(value, 0) - 1;
                if (newCount == 0) {
                    counts.remove(value);
                } else {
                    counts.put(value, newCount);
                }
            }

            if (counts.isEmpty() == false) {
                var extraValuesMessage = new StringBuilder("extra values: ");
                for (var entry : counts.entrySet()) {
                    extraValuesMessage.append('\n').append(entry.getKey()).append(": ").append(entry.getValue());
                }

                return MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Values of type [counted_keyword] don't match, "
                            + extraValuesMessage
                            + ".\n"
                            + prettyPrintCollections(actualNormalized, expectedNormalized)
                    )
                );
            }

            return MatchResult.match();
        }

        private static List<String> normalize(List<Object> values) {
            return values.stream().filter(Objects::nonNull).map(it -> (String) it).toList();
        }
    }

    class HalfFloatMatcher extends GenericMappingAwareMatcher {
        HalfFloatMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            super("half_float", actualMappings, actualSettings, expectedMappings, expectedSettings);
        }

        @Override
        Object convert(Object value, Object nullValue) {
            var nullValueShort = nullValue != null ? HalfFloatPoint.halfFloatToSortableShort(((Number) nullValue).floatValue()) : null;

            return switch (value) {
                case null -> nullValueShort;
                case Number n -> HalfFloatPoint.halfFloatToSortableShort(n.floatValue());
                case String s -> {
                    // Special case for number coercion from strings
                    if (s.isEmpty()) {
                        yield nullValueShort;
                    }

                    try {
                        var f = Float.parseFloat(s);
                        yield HalfFloatPoint.halfFloatToSortableShort(f);
                    } catch (NumberFormatException e) {
                        // Malformed, leave it be and match as is
                        yield s;
                    }
                }
                default -> value;
            };
        }
    }

    class ScaledFloatMatcher implements FieldSpecificMatcher {
        private final XContentBuilder actualMappings;
        private final Settings.Builder actualSettings;
        private final XContentBuilder expectedMappings;
        private final Settings.Builder expectedSettings;

        ScaledFloatMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            this.actualMappings = actualMappings;
            this.actualSettings = actualSettings;
            this.expectedMappings = expectedMappings;
            this.expectedSettings = expectedSettings;
        }

        @Override
        public MatchResult match(
            List<Object> actual,
            List<Object> expected,
            Map<String, Object> actualMapping,
            Map<String, Object> expectedMapping
        ) {
            var scalingFactor = getMappingParameter("scaling_factor", actualMapping, expectedMapping);

            assert scalingFactor instanceof Number;
            double scalingFactorDouble = ((Number) scalingFactor).doubleValue();

            var nullValue = (Number) getNullValue(actualMapping, expectedMapping);

            // It is possible that we receive a mix of reduced precision values and original values.
            // F.e. in case of `synthetic_source_keep: "arrays"` in nested objects only arrays are preserved as is
            // and therefore any singleton values have reduced precision.
            // Therefore, we need to match either an exact value or a normalized value.
            var expectedNumbers = numbers(expected, nullValue);
            var actualNumbers = numbers(actual, nullValue);

            for (var expectedValue : expectedNumbers) {
                if (actualNumbers.contains(expectedValue) == false
                    && actualNumbers.contains(encodeDecodeWithPrecisionLoss(expectedValue, scalingFactorDouble)) == false) {
                    return MatchResult.noMatch(
                        formatErrorMessage(
                            actualMappings,
                            actualSettings,
                            expectedMappings,
                            expectedSettings,
                            "Values of type [scaled_float] don't match after normalization, normalized "
                                + prettyPrintCollections(actualNumbers, expectedNumbers)
                        )
                    );
                }
            }

            var expectedNotNumbers = notNumbers(expected);
            var actualNotNumbers = notNumbers(actual);
            for (var expectedValue : expectedNotNumbers) {
                if (actualNotNumbers.contains(expectedValue) == false) {
                    return MatchResult.noMatch(
                        formatErrorMessage(
                            actualMappings,
                            actualSettings,
                            expectedMappings,
                            expectedSettings,
                            "Malformed values of [scaled_float] field don't match, values:"
                                + prettyPrintCollections(actualNotNumbers, expectedNotNumbers)
                        )
                    );
                }
            }

            return MatchResult.match();
        }

        private Set<Double> numbers(List<Object> values, Number nullValue) {
            if (values == null) {
                return Set.of();
            }

            return values.stream()
                .map(v -> convertNumber(v, nullValue))
                .filter(Objects::nonNull)
                .map(ScaledFloatMatcher::toDouble)
                .collect(Collectors.toSet());
        }

        private static Object convertNumber(Object value, Number nullValue) {
            if (value == null) {
                return nullValue;
            }
            // Special case for number coercion from strings
            if (value instanceof String s && s.isEmpty()) {
                return nullValue;
            }
            if (value instanceof Number n) {
                return n;
            }

            return null;
        }

        private Set<Object> notNumbers(List<Object> values) {
            if (values == null) {
                return Set.of();
            }

            return values.stream()
                .filter(Objects::nonNull)
                .filter(v -> v instanceof Number == false)
                .filter(v -> v instanceof String == false || ((String) v).isEmpty() == false)
                .collect(Collectors.toSet());
        }

        private Double encodeDecodeWithPrecisionLoss(double value, double scalingFactor) {
            // Based on logic in ScaledFloatFieldMapper
            var encoded = Math.round(value * scalingFactor);
            return encoded / scalingFactor;
        }

        private static double toDouble(Object value) {
            return ((Number) value).doubleValue();
        }
    }

    class UnsignedLongMatcher extends GenericMappingAwareMatcher {
        UnsignedLongMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            super("unsigned_long", actualMappings, actualSettings, expectedMappings, expectedSettings);
        }

        @Override
        Object convert(Object value, Object nullValue) {
            var nullValueBigInt = nullValue != null ? BigInteger.valueOf(((Number) nullValue).longValue()) : null;

            return switch (value) {
                case null -> nullValueBigInt;
                case String s -> {
                    // Special case for number coercion from strings
                    if (s.isEmpty()) {
                        yield nullValueBigInt;
                    }

                    yield s;
                }
                case Long l -> BigInteger.valueOf(l);
                default -> value;
            };

        }
    }

    class KeywordMatcher extends GenericMappingAwareMatcher {
        KeywordMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            super("keyword", actualMappings, actualSettings, expectedMappings, expectedSettings);
        }

        @Override
        Object convert(Object value, Object nullValue) {
            if (value == null) {
                return nullValue;
            }

            return value;
        }
    }

    class NumberMatcher extends GenericMappingAwareMatcher {
        NumberMatcher(
            String fieldType,
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            super(fieldType, actualMappings, actualSettings, expectedMappings, expectedSettings);
        }

        @Override
        Object convert(Object value, Object nullValue) {
            if (value == null) {
                return nullValue;
            }
            // Special case for number coercion from strings
            if (value instanceof String s && s.isEmpty()) {
                return nullValue;
            }

            return value;
        }
    }

    class BooleanMatcher extends GenericMappingAwareMatcher {
        BooleanMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            super("boolean", actualMappings, actualSettings, expectedMappings, expectedSettings);
        }

        @Override
        Object convert(Object value, Object nullValue) {
            Boolean nullValueBool = null;
            if (nullValue != null) {
                nullValueBool = nullValue instanceof Boolean b ? b : Boolean.parseBoolean((String) nullValue);
            }

            if (value == null) {
                return nullValueBool;
            }
            if (value instanceof String s && s.isEmpty()) {
                // This a documented behavior.
                return false;
            }
            if (value instanceof String s) {
                try {
                    return Boolean.parseBoolean(s);
                } catch (Exception e) {
                    // malformed
                    return value;
                }
            }

            return value;
        }
    }

    class DateMatcher implements FieldSpecificMatcher {
        private final XContentBuilder actualMappings;
        private final Settings.Builder actualSettings;
        private final XContentBuilder expectedMappings;
        private final Settings.Builder expectedSettings;

        DateMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            this.actualMappings = actualMappings;
            this.actualSettings = actualSettings;
            this.expectedMappings = expectedMappings;
            this.expectedSettings = expectedSettings;
        }

        @Override
        public MatchResult match(
            List<Object> actual,
            List<Object> expected,
            Map<String, Object> actualMapping,
            Map<String, Object> expectedMapping
        ) {
            var format = (String) getMappingParameter("format", actualMapping, expectedMapping);
            var nullValue = getNullValue(actualMapping, expectedMapping);

            Function<Object, Object> convert = v -> convert(v, nullValue);
            if (format != null) {
                var formatter = DateTimeFormatter.ofPattern(format, Locale.ROOT).withZone(ZoneId.from(ZoneOffset.UTC));
                convert = v -> convert(v, nullValue, formatter);
            }

            var actualNormalized = normalize(actual, convert);
            var expectedNormalized = normalize(expected, convert);

            return actualNormalized.equals(expectedNormalized)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Values of type [date] don't match after normalization, normalized "
                            + prettyPrintCollections(actualNormalized, expectedNormalized)
                    )
                );
        }

        private Set<Object> normalize(List<Object> values, Function<Object, Object> convert) {
            if (values == null) {
                return Set.of();
            }

            return values.stream().map(convert).filter(Objects::nonNull).collect(Collectors.toSet());
        }

        Object convert(Object value, Object nullValue) {
            if (value == null) {
                return nullValue == null ? null : Instant.ofEpochMilli((Long) nullValue);
            }
            if (value instanceof Integer i) {
                return Instant.ofEpochMilli(i);
            }
            if (value instanceof Long l) {
                return Instant.ofEpochMilli(l);
            }

            assert value instanceof String;
            try {
                // values from synthetic source will be formatted with default formatter
                return Instant.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse((String) value));
            } catch (Exception e) {
                // malformed
                return value;
            }
        }

        Object convert(Object value, Object nullValue, DateTimeFormatter dateTimeFormatter) {
            if (value == null) {
                return nullValue == null ? null : Instant.from(dateTimeFormatter.parse((String) nullValue)).toEpochMilli();
            }

            assert value instanceof String;
            try {
                return Instant.from(dateTimeFormatter.parse((String) value)).toEpochMilli();
            } catch (Exception e) {
                // malformed
                return value;
            }
        }
    }

    class GeoPointMatcher extends GenericMappingAwareMatcher {
        GeoPointMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            super("geo_point", actualMappings, actualSettings, expectedMappings, expectedSettings);
        }

        @Override
        @SuppressWarnings("unchecked")
        Object convert(Object value, Object nullValue) {
            if (value == null) {
                if (nullValue != null) {
                    return normalizePoint(new GeoPoint((String) nullValue));
                }
                return null;
            }
            if (value instanceof String s) {
                try {
                    return normalizePoint(new GeoPoint(s));
                } catch (Exception e) {
                    // malformed
                    return value;
                }
            }
            if (value instanceof Map<?, ?> m) {
                if (m.get("type") != null) {
                    var coordinates = (List<Double>) m.get("coordinates");
                    // Order in GeoJSON is lon,lat
                    return normalizePoint(new GeoPoint(coordinates.get(1), coordinates.get(0)));
                } else {
                    return normalizePoint(new GeoPoint((Double) m.get("lat"), (Double) m.get("lon")));
                }
            }
            if (value instanceof List<?> l) {
                // Order in arrays is lon,lat
                return normalizePoint(new GeoPoint((Double) l.get(1), (Double) l.get(0)));
            }

            return value;
        }

        private static GeoPoint normalizePoint(GeoPoint point) {
            return point.resetFromEncoded(point.getEncoded());
        }
    }

    class ExactMatcher implements FieldSpecificMatcher {
        private final String fieldType;
        private final XContentBuilder actualMappings;
        private final Settings.Builder actualSettings;
        private final XContentBuilder expectedMappings;
        private final Settings.Builder expectedSettings;

        ExactMatcher(
            String fieldType,
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            this.fieldType = fieldType;
            this.actualMappings = actualMappings;
            this.actualSettings = actualSettings;
            this.expectedMappings = expectedMappings;
            this.expectedSettings = expectedSettings;
        }

        @Override
        public MatchResult match(
            List<Object> actual,
            List<Object> expected,
            Map<String, Object> actualMapping,
            Map<String, Object> expectedMapping
        ) {
            return actual.equals(expected)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Values of type ["
                            + fieldType
                            + "] were expected to match exactly "
                            + "but don't match, values "
                            + prettyPrintCollections(actual, expected)
                    )
                );
        }
    }

    class TextMatcher implements FieldSpecificMatcher {
        private final XContentBuilder actualMappings;
        private final Settings.Builder actualSettings;
        private final XContentBuilder expectedMappings;
        private final Settings.Builder expectedSettings;

        TextMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            this.actualMappings = actualMappings;
            this.actualSettings = actualSettings;
            this.expectedMappings = expectedMappings;
            this.expectedSettings = expectedSettings;
        }

        @Override
        @SuppressWarnings("unchecked")
        public MatchResult match(
            List<Object> actual,
            List<Object> expected,
            Map<String, Object> actualMapping,
            Map<String, Object> expectedMapping
        ) {
            var expectedNormalized = normalize(expected);
            var actualNormalized = normalize(actual);

            // Match simply as text first.
            if (actualNormalized.equals(expectedNormalized)) {
                return MatchResult.match();
            }

            // In some cases synthetic source for text fields is synthesized using the keyword multi field.
            // So in this case it's appropriate to match it using keyword matching logic (mainly to cover `null_value`).
            var multiFields = (Map<String, Object>) getMappingParameter("fields", actualMapping, expectedMapping);
            if (multiFields != null) {
                var keywordMatcher = new KeywordMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings);

                var keywordFieldMapping = (Map<String, Object>) multiFields.get("kwd");
                var keywordMatchResult = keywordMatcher.match(actual, expected, keywordFieldMapping, keywordFieldMapping);
                if (keywordMatchResult.isMatch()) {
                    return MatchResult.match();
                }
            }

            return MatchResult.noMatch(
                formatErrorMessage(
                    actualMappings,
                    actualSettings,
                    expectedMappings,
                    expectedSettings,
                    "Values of type [text] don't match, " + prettyPrintCollections(actual, expected)
                )
            );
        }

        private Set<Object> normalize(List<Object> values) {
            if (values == null) {
                return Set.of();
            }

            return values.stream().filter(Objects::nonNull).collect(Collectors.toSet());
        }
    }

    class IpMatcher extends GenericMappingAwareMatcher {
        IpMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            super("ip", actualMappings, actualSettings, expectedMappings, expectedSettings);
        }

        @Override
        Object convert(Object value, Object nullValue) {
            if (value == null) {
                if (nullValue != null) {
                    return nullValue;
                }
                return null;
            }

            // We should be always able to convert an IP back to original string.
            return value;
        }
    }

    class ConstantKeywordMatcher extends GenericMappingAwareMatcher {
        ConstantKeywordMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            super("constant_keyword", actualMappings, actualSettings, expectedMappings, expectedSettings);
        }

        @Override
        Object convert(Object value, Object nullValue) {
            // We just need to get rid of literal `null`s which is done in the caller.
            return value;
        }
    }

    class WildcardMatcher extends GenericMappingAwareMatcher {
        WildcardMatcher(
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            super("wildcard", actualMappings, actualSettings, expectedMappings, expectedSettings);
        }

        @Override
        Object convert(Object value, Object nullValue) {
            if (value == null) {
                if (nullValue != null) {
                    return nullValue;
                }
                return null;
            }

            return value;
        }
    }

    /**
     * Generic matcher that supports common matching logic like null values.
     */
    abstract class GenericMappingAwareMatcher implements FieldSpecificMatcher {
        private final String fieldType;

        private final XContentBuilder actualMappings;
        private final Settings.Builder actualSettings;
        private final XContentBuilder expectedMappings;
        private final Settings.Builder expectedSettings;

        GenericMappingAwareMatcher(
            String fieldType,
            XContentBuilder actualMappings,
            Settings.Builder actualSettings,
            XContentBuilder expectedMappings,
            Settings.Builder expectedSettings
        ) {
            this.fieldType = fieldType;
            this.actualMappings = actualMappings;
            this.actualSettings = actualSettings;
            this.expectedMappings = expectedMappings;
            this.expectedSettings = expectedSettings;
        }

        @Override
        @SuppressWarnings("unchecked")
        public MatchResult match(
            List<Object> actual,
            List<Object> expected,
            Map<String, Object> actualMapping,
            Map<String, Object> expectedMapping
        ) {
            var nullValue = getNullValue(actualMapping, expectedMapping);

            var expectedNormalized = normalize(expected, nullValue);
            var actualNormalized = normalize(actual, nullValue);

            return actualNormalized.equals(expectedNormalized)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Values of type ["
                            + fieldType
                            + "] don't match after normalization, normalized "
                            + prettyPrintCollections(actualNormalized, expectedNormalized)
                    )
                );
        }

        private Set<Object> normalize(List<Object> values, Object nullValue) {
            if (values == null) {
                return Set.of();
            }

            return values.stream().map(v -> convert(v, nullValue)).filter(Objects::nonNull).collect(Collectors.toSet());
        }

        abstract Object convert(Object value, Object nullValue);
    }

    private static Object getNullValue(Map<String, Object> actualMapping, Map<String, Object> expectedMapping) {
        return getMappingParameter("null_value", actualMapping, expectedMapping);
    }

    private static Object getMappingParameter(String name, Map<String, Object> actualMapping, Map<String, Object> expectedMapping) {
        var actualValue = actualMapping.get(name);
        var expectedValue = expectedMapping.get(name);
        if (Objects.equals(actualValue, expectedValue) == false) {
            throw new IllegalStateException("[" + name + "] parameter does not match between actual and expected mapping");
        }
        return actualValue;
    }
}
