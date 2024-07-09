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
import org.elasticsearch.datastreams.logsdb.qa.exceptions.UncomparableMatcherException;
import org.elasticsearch.xcontent.XContentBuilder;

public class EqualMatcher<T> extends Matcher<T> {
    private final XContentBuilder actualMappings;
    private final Settings.Builder actualSettings;
    private final XContentBuilder expectedMappings;
    private final Settings.Builder expectedSettings;
    private final T actual;
    private final T expected;

    public EqualMatcher(
        XContentBuilder actualMappings,
        Settings.Builder actualSettings,
        XContentBuilder expectedMappings,
        Settings.Builder expectedSettings,
        T actual,
        T expected
    ) {
        this.actualMappings = actualMappings;
        this.actualSettings = actualSettings;
        this.expectedMappings = expectedMappings;
        this.expectedSettings = expectedSettings;
        this.actual = actual;
        this.expected = expected;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean match() throws MatcherException {
        if (actual == null) {
            if (expected == null) {
                throw new UncomparableMatcherException(
                    actualMappings,
                    actualSettings,
                    expectedMappings,
                    expectedSettings,
                    "Both null");
            }
            return false;
        }
        if (expected == null) {
            return false;
        }
        if (actual.getClass().equals(expected.getClass())) {
           throw new MismatchTypeMatcherException();
       }
        if (actual.getClass().isArray() && expected.getClass().isArray()) {
            return matchArraysEqual((T[]) actual, (T[]) expected);
        }
        if (actual.getClass().isArray() == false && expected.getClass().isArray() == false) {
            return matchObjectEqual(actual, expected);
        }
        throw new MismatchTypeMatcherException();
    }

    private boolean matchObjectEqual(final T actual, final T expected) {
        return actual.equals(expected);
    }

    private boolean matchArraysEqual(final T[] actualArray, final T[] expectedArray) {
        if (actualArray.length != expectedArray.length) {
            return false;
        }
        for (int i = 0; i < actualArray.length; i++) {
            boolean isEqual = matchObjectEqual(actualArray[i], expectedArray[i]);
            if (isEqual == false) {
                return false;
            }
        }
        return true;
    }
}
