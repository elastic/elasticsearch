/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.matchers.source;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logsdb.datageneration.matchers.ListEqualMatcher;
import org.elasticsearch.logsdb.datageneration.matchers.MatchResult;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.logsdb.datageneration.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.logsdb.datageneration.matchers.Messages.prettyPrintCollections;

class DynamicFieldMatcher {
    private final XContentBuilder actualMappings;
    private final Settings.Builder actualSettings;
    private final XContentBuilder expectedMappings;
    private final Settings.Builder expectedSettings;

    DynamicFieldMatcher(
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

    /**
     * Performs matching of dynamically mapped field values if they need special treatment.
     * @return {#{@link MatchResult}} if field values need special treatment by this matcher.
     * If field values can be matched using generic mapper, returns {@link Optional#empty()}.
     */
    public MatchResult match(List<Object> actual, List<Object> expected) {
        if (expected == null) {
            expected = List.of();
        }
        if (actual == null) {
            actual = List.of();
        }

        // Floating point values are always mapped as float with dynamic mapping.
        var isDouble = expected.stream().filter(Objects::nonNull).findFirst().map(o -> o instanceof Double).orElse(false);
        if (isDouble) {
            assert expected.stream().allMatch(o -> o == null || o instanceof Double);

            var normalizedActual = normalizeDoubles(actual);
            var normalizedExpected = normalizeDoubles(expected);

            return normalizedActual.equals(normalizedExpected)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Values of dynamically mapped field containing double values don't match after normalization, normalized "
                            + prettyPrintCollections(normalizedActual, normalizedExpected)
                    )
                );
        }

        return matchWithGenericMatcher(actual, expected);
    }

    private static Set<Float> normalizeDoubles(List<Object> values) {
        if (values == null) {
            return Set.of();
        }

        Function<Object, Float> toFloat = (o) -> o instanceof Number n ? n.floatValue() : Float.parseFloat((String) o);
        return values.stream().filter(Objects::nonNull).map(toFloat).collect(Collectors.toSet());
    }

    private MatchResult matchWithGenericMatcher(List<Object> actualValues, List<Object> expectedValues) {
        var genericListMatcher = new ListEqualMatcher(
            actualMappings,
            actualSettings,
            expectedMappings,
            expectedSettings,
            SourceTransforms.normalizeValues(actualValues),
            SourceTransforms.normalizeValues(expectedValues),
            true
        );

        return genericListMatcher.match();
    }
}
