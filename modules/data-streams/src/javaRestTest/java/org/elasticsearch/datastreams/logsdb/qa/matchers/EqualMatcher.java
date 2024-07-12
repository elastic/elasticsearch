/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.matchers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.logsdb.qa.exceptions.MatcherException;
import org.elasticsearch.datastreams.logsdb.qa.exceptions.MismatchTypeMatcherException;
import org.elasticsearch.datastreams.logsdb.qa.exceptions.UncomparableMatcherException;
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
    public boolean match() throws MatcherException {
        if (actual == null) {
            if (expected == null) {
                throw new UncomparableMatcherException(
                    actualMappings,
                    actualSettings,
                    expectedMappings,
                    expectedSettings,
                    "Both 'actual' and 'expected' are null"
                );
            }
            return false;
        }
        if (expected == null) {
            return false;
        }
        if (actual.getClass().equals(expected.getClass()) == false) {
            throw new MismatchTypeMatcherException(
                actualMappings,
                actualSettings,
                expectedMappings,
                expectedSettings,
                "Unable to match " + actual.getClass().getSimpleName() + " to " + expected.getClass().getSimpleName()
            );
        }
        if (actual.getClass().isArray()) {
            return matchArraysEqual((T[]) actual, (T[]) expected, ignoringSort);
        }
        if (actual instanceof List<?> act && expected instanceof List<?> exp) {
            return matchArraysEqual((T[]) (act).toArray(), (T[]) (exp).toArray(), ignoringSort);
        }
        return actual.equals(expected);
    }

    private boolean matchArraysEqual(final T[] actualArray, final T[] expectedArray, boolean ignoreSorting) {
        if (actualArray.length != expectedArray.length) {
            return false;
        }
        if (ignoreSorting) {
            return matchArraysEqualIgnoringSorting(actualArray, expectedArray) != false;
        } else {
            return matchArraysEqualExact(actualArray, expectedArray) != false;
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
                return true;
            }
        }
        return false;
    }
}
