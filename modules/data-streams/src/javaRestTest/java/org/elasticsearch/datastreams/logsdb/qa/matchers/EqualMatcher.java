/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.matchers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Arrays;
import java.util.List;

class EqualMatcher<T> extends Matcher {
    private final XContentBuilder actualMappings;
    private final Settings.Builder actualSettings;
    private final XContentBuilder expectedMappings;
    private final Settings.Builder expectedSettings;
    private final T actual;
    private final T expected;
    private final boolean ignoringSort;

    EqualMatcher(
        XContentBuilder actualMappings,
        Settings.Builder actualSettings,
        XContentBuilder expectedMappings,
        Settings.Builder expectedSettings,
        T actual,
        T expected,
        boolean ignoringSort
    ) {
        this.actualMappings = actualMappings;
        this.actualSettings = actualSettings;
        this.expectedMappings = expectedMappings;
        this.expectedSettings = expectedSettings;
        this.actual = actual;
        this.expected = expected;
        this.ignoringSort = ignoringSort;
    }

    @SuppressWarnings("unchecked")
    public MatchResult match() {
        if (actual == null) {
            if (expected == null) {

                return MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Both 'actual' and 'expected' are null"
                    )
                );
            }
            return MatchResult.noMatch(
                formatErrorMessage(actualMappings, actualSettings, expectedMappings, expectedSettings, "Expected is null but actual is not")
            );
        }
        if (expected == null) {
            return MatchResult.noMatch(
                formatErrorMessage(actualMappings, actualSettings, expectedMappings, expectedSettings, "Actual is null but expected is not")
            );
        }
        if (actual.getClass().equals(expected.getClass()) == false) {
            return MatchResult.noMatch(
                formatErrorMessage(
                    actualMappings,
                    actualSettings,
                    expectedMappings,
                    expectedSettings,
                    "Unable to match " + actual.getClass().getSimpleName() + " to " + expected.getClass().getSimpleName()
                )
            );
        }
        if (actual.getClass().isArray()) {
            return matchArraysEqual((T[]) actual, (T[]) expected, ignoringSort);
        }
        if (actual instanceof List<?> act && expected instanceof List<?> exp) {
            return matchArraysEqual((T[]) (act).toArray(), (T[]) (exp).toArray(), ignoringSort);
        }
        return actual.equals(expected)
            ? MatchResult.match()
            : MatchResult.noMatch(
                formatErrorMessage(actualMappings, actualSettings, expectedMappings, expectedSettings, "Actual does not equal expected")
            );
    }

    private MatchResult matchArraysEqual(final T[] actualArray, final T[] expectedArray, boolean ignoreSorting) {
        if (actualArray.length != expectedArray.length) {
            return MatchResult.noMatch(
                formatErrorMessage(actualMappings, actualSettings, expectedMappings, expectedSettings, "Array lengths do no match")
            );
        }
        if (ignoreSorting) {
            return matchArraysEqualIgnoringSorting(actualArray, expectedArray)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Arrays do not match when ignoreing sort order"
                    )
                );
        } else {
            return matchArraysEqualExact(actualArray, expectedArray)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(actualMappings, actualSettings, expectedMappings, expectedSettings, "Arrays do not match exactly")
                );
        }
    }

    private static <T> boolean matchArraysEqualIgnoringSorting(T[] actualArray, T[] expectedArray) {
        final List<T> actualList = Arrays.asList(actualArray);
        final List<T> expectedList = Arrays.asList(expectedArray);
        return actualList.containsAll(expectedList) && expectedList.containsAll(actualList);
    }

    private static <T> boolean matchArraysEqualExact(T[] actualArray, T[] expectedArray) {
        for (int i = 0; i < actualArray.length; i++) {
            boolean isEqual = actualArray[i].equals(expectedArray[i]);
            if (isEqual == false) {
                return false;
            }
        }
        return true;
    }
}
