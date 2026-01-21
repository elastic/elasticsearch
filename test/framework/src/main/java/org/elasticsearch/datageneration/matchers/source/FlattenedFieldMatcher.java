/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.matchers.source;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.matchers.MatchResult;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.datageneration.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.datageneration.matchers.Messages.prettyPrintCollections;

public class FlattenedFieldMatcher implements FieldSpecificMatcher {
    private final XContentBuilder actualMappings;
    private final Settings.Builder actualSettings;
    private final XContentBuilder expectedMappings;
    private final Settings.Builder expectedSettings;

    public FlattenedFieldMatcher(
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
        var nullValue = FieldSpecificMatcher.getNullValue(actualMapping, expectedMapping);

        @SuppressWarnings("unchecked")
        FlattenedSourceMatcher matcher = new FlattenedSourceMatcher(
            actualMappings,
            actualSettings,
            expectedMappings,
            expectedSettings,
            actual.stream().map(o -> (Map<String, Object>) o).toList(),
            expected.stream().map(o -> (Map<String, Object>) o).toList(),
            true,
            nullValue
        );

        return matcher.match();
    }

    private static class FlattenedSourceMatcher extends SourceMatcher {
        private final Object nullValue;

        private FlattenedSourceMatcher(
            final XContentBuilder actualMappings,
            final Settings.Builder actualSettings,
            final XContentBuilder expectedMappings,
            final Settings.Builder expectedSettings,
            final List<Map<String, Object>> actual,
            final List<Map<String, Object>> expected,
            final boolean ignoringSort,
            final Object nullValue
        ) {
            super(
                Collections.emptyMap(),
                actualMappings,
                actualSettings,
                expectedMappings,
                expectedSettings,
                actual,
                expected,
                ignoringSort
            );

            this.nullValue = nullValue;
        }

        @Override
        protected MatchResult matchWithFieldSpecificMatcher(String fieldName, List<Object> actualValues, List<Object> expectedValues) {
            var expectedNormalized = normalize(expectedValues);
            var actualNormalized = normalize(actualValues);

            return actualNormalized.equals(expectedNormalized)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Values of type [flattened] don't match after normalization, normalized "
                            + prettyPrintCollections(actualNormalized, expectedNormalized)
                    )
                );
        }

        private Set<Object> normalize(List<Object> values) {
            if (values == null) {
                return Collections.emptySet();
            }

            return values.stream()
                .map(v -> v == null ? nullValue : v)
                .filter(Objects::nonNull)
                .map(Object::toString)
                .collect(Collectors.toSet());
        }
    }
}
