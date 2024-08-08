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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.datastreams.logsdb.qa.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.datastreams.logsdb.qa.matchers.Messages.prettyPrintCollections;

interface FieldSpecificMatcher {
    MatchResult match(List<Object> actual, List<Object> expected);

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
        public MatchResult match(List<Object> actual, List<Object> expected) {
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
}
