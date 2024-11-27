/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa.matchers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.List;

import static org.elasticsearch.xpack.logsdb.qa.matchers.Messages.formatErrorMessage;

public class GenericEqualsMatcher<T> extends Matcher {
    protected final XContentBuilder actualMappings;
    protected final Settings.Builder actualSettings;
    protected final XContentBuilder expectedMappings;
    protected final Settings.Builder expectedSettings;
    protected final T actual;
    protected final T expected;
    protected final boolean ignoringSort;

    protected GenericEqualsMatcher(
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
            return new ArrayEqualMatcher(
                actualMappings,
                actualSettings,
                expectedMappings,
                expectedSettings,
                (Object[]) actual,
                (Object[]) expected,
                ignoringSort
            ).match();
        }
        if (actual instanceof List<?> act && expected instanceof List<?> exp) {
            return new ListEqualMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings, act, exp, ignoringSort).match();
        }
        return new ObjectMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings, actual, expected).match();
    }
}
