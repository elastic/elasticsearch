/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.matchers.source;

import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.logsdb.qa.matchers.MatchResult;
import org.elasticsearch.xcontent.XContentBuilder;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.datastreams.logsdb.qa.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.datastreams.logsdb.qa.matchers.Messages.prettyPrintCollections;

interface FieldSpecificMatcher {
    MatchResult match(List<Object> actual, List<Object> expected, Map<String, Object> actualMapping, Map<String, Object> expectedMapping);

    class HalfFloatMatcher implements FieldSpecificMatcher {
        private final XContentBuilder actualMappings;
        private final Settings.Builder actualSettings;
        private final XContentBuilder expectedMappings;
        private final Settings.Builder expectedSettings;

        HalfFloatMatcher(
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
            var actualHalfFloatBytes = normalize(actual);
            var expectedHalfFloatBytes = normalize(expected);

            return actualHalfFloatBytes.equals(expectedHalfFloatBytes)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Values of type [half_float] don't match after normalization, normalized "
                            + prettyPrintCollections(actualHalfFloatBytes, expectedHalfFloatBytes)
                    )
                );
        }

        private static Set<Short> normalize(List<Object> values) {
            if (values == null) {
                return Set.of();
            }

            Function<Object, Float> toFloat = (o) -> o instanceof Number n ? n.floatValue() : Float.parseFloat((String) o);
            return values.stream()
                .filter(Objects::nonNull)
                .map(toFloat)
                // Based on logic in NumberFieldMapper
                .map(HalfFloatPoint::halfFloatToSortableShort)
                .collect(Collectors.toSet());
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
            var scalingFactor = actualMapping.get("scaling_factor");
            var expectedScalingFactor = expectedMapping.get("scaling_factor");
            if (Objects.equals(scalingFactor, expectedScalingFactor) == false) {
                throw new IllegalStateException("Scaling factor for scaled_float field does not match between actual and expected mapping");
            }

            assert scalingFactor instanceof Number;
            var expectedNormalized = normalizeExpected(expected, ((Number) scalingFactor).doubleValue());
            var actualNormalized = normalizeActual(actual);

            return actualNormalized.equals(expectedNormalized)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Values of type [scaled_float] don't match after normalization, normalized "
                            + prettyPrintCollections(actualNormalized, expectedNormalized)
                    )
                );
        }

        private static Set<Double> normalizeExpected(List<Object> values, double scalingFactor) {
            if (values == null) {
                return Set.of();
            }

            return values.stream()
                .filter(Objects::nonNull)
                .map(ScaledFloatMatcher::toDouble)
                // Based on logic in ScaledFloatFieldMapper
                .map(v -> {
                    var encoded = Math.round(v * scalingFactor);
                    return encoded / scalingFactor;
                })
                .collect(Collectors.toSet());
        }

        private static Set<Double> normalizeActual(List<Object> values) {
            if (values == null) {
                return Set.of();
            }

            return values.stream().filter(Objects::nonNull).map(ScaledFloatMatcher::toDouble).collect(Collectors.toSet());
        }

        private static double toDouble(Object value) {
            return ((Number) value).doubleValue();
        }
    }

    class UnsignedLongMatcher implements FieldSpecificMatcher {
        private final XContentBuilder actualMappings;
        private final Settings.Builder actualSettings;
        private final XContentBuilder expectedMappings;
        private final Settings.Builder expectedSettings;

        UnsignedLongMatcher(
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
            var expectedNormalized = normalize(expected);
            var actualNormalized = normalize(actual);

            return actualNormalized.equals(expectedNormalized)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Values of type [scaled_float] don't match after normalization, normalized "
                            + prettyPrintCollections(actualNormalized, expectedNormalized)
                    )
                );
        }

        private static Set<BigInteger> normalize(List<Object> values) {
            if (values == null) {
                return Set.of();
            }

            return values.stream().filter(Objects::nonNull).map(UnsignedLongMatcher::toBigInteger).collect(Collectors.toSet());
        }

        private static BigInteger toBigInteger(Object value) {
            if (value instanceof String s) {
                return new BigInteger(s, 10);
            }
            if (value instanceof Long l) {
                return BigInteger.valueOf(l);
            }

            return (BigInteger) value;
        }
    }
}
