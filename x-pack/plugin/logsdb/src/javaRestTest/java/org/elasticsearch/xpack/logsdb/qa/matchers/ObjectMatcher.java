/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa.matchers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import static org.elasticsearch.xpack.logsdb.qa.matchers.Messages.formatErrorMessage;

public class ObjectMatcher extends GenericEqualsMatcher<Object> {
    ObjectMatcher(
        final XContentBuilder actualMappings,
        final Settings.Builder actualSettings,
        final XContentBuilder expectedMappings,
        final Settings.Builder expectedSettings,
        final Object actual,
        final Object expected
    ) {
        super(actualMappings, actualSettings, expectedMappings, expectedSettings, actual, expected, true);
    }

    @Override
    public MatchResult match() {
        return actual.equals(expected)
            ? MatchResult.match()
            : MatchResult.noMatch(
                formatErrorMessage(
                    actualMappings,
                    actualSettings,
                    expectedMappings,
                    expectedSettings,
                    "Actual does not equal expected, actual: " + actual + ", expected: " + expected
                )
            );
    }
}
