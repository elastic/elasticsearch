/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.matchers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.datageneration.matchers.Messages.formatErrorMessage;
import static org.elasticsearch.datageneration.matchers.Messages.prettyPrintArrays;

class ArrayEqualMatcher extends GenericEqualsMatcher<Object[]> {
    ArrayEqualMatcher(
        final XContentBuilder actualMappings,
        final Settings.Builder actualSettings,
        final XContentBuilder expectedMappings,
        final Settings.Builder expectedSettings,
        final Object[] actual,
        final Object[] expected,
        boolean ignoringSort
    ) {
        super(actualMappings, actualSettings, expectedMappings, expectedSettings, actual, expected, ignoringSort);
    }

    @Override
    public MatchResult match() {
        return matchArraysEqual(actual, expected, ignoringSort);
    }

    private MatchResult matchArraysEqual(final Object[] actualArray, final Object[] expectedArray, boolean ignoreSorting) {
        if (actualArray.length != expectedArray.length) {
            return MatchResult.noMatch(
                formatErrorMessage(
                    actualMappings,
                    actualSettings,
                    expectedMappings,
                    expectedSettings,
                    "Array lengths do no match, " + prettyPrintArrays(actualArray, expectedArray)
                )
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
                        "Arrays do not match when ignoring sort order, " + prettyPrintArrays(actualArray, expectedArray)
                    )
                );
        } else {
            return matchArraysEqualExact(actualArray, expectedArray)
                ? MatchResult.match()
                : MatchResult.noMatch(
                    formatErrorMessage(
                        actualMappings,
                        actualSettings,
                        expectedMappings,
                        expectedSettings,
                        "Arrays do not match exactly, " + prettyPrintArrays(actualArray, expectedArray)
                    )
                );
        }
    }

    private static boolean matchArraysEqualIgnoringSorting(final Object[] actualArray, final Object[] expectedArray) {
        final List<Object> actualList = Arrays.asList(actualArray);
        final List<Object> expectedList = Arrays.asList(expectedArray);
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
