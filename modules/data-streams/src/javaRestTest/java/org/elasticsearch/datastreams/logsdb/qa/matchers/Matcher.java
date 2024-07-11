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
import org.elasticsearch.datastreams.logsdb.qa.exceptions.NotEqualMatcherException;
import org.elasticsearch.xcontent.XContentBuilder;

/**
 * A base class to be used for the matching logic when comparing query results.
 */
public abstract class Matcher {

    public static <T> SettingsStep<T> mappings(final XContentBuilder actualMappings, final XContentBuilder expectedMappings) {
        return new Builder<>(expectedMappings, actualMappings);
    }

    public interface SettingsStep<T> {
        ExpectedStep<T> settings(Settings.Builder actualSettings, Settings.Builder expectedSettings);
    }

    public interface ExpectedStep<T> {
        CompareStep<T> expected(T expected);
    }

    public interface CompareStep<T> {
        void isEqualTo(T actual) throws MatcherException;

        CompareStep<T> ignoringSort(boolean ignoringSort);
    }

    private static class Builder<T> implements SettingsStep<T>, CompareStep<T>, ExpectedStep<T> {

        private final XContentBuilder expectedMappings;
        private final XContentBuilder actualMappings;
        private Settings.Builder expectedSettings;
        private Settings.Builder actualSettings;
        private T expected;
        private T actual;
        private boolean ignoringSort;

        @Override
        public ExpectedStep<T> settings(Settings.Builder actualSettings, Settings.Builder expectedSettings) {
            this.actualSettings = actualSettings;
            this.expectedSettings = expectedSettings;
            return this;
        }

        private Builder(
            final XContentBuilder actualMappings,
            final XContentBuilder expectedMappings

        ) {
            this.actualMappings = actualMappings;
            this.expectedMappings = expectedMappings;
        }

        @Override
        public void isEqualTo(T actual) throws MatcherException {
            boolean match = new EqualMatcher<>(
                actualMappings,
                actualSettings,
                expectedMappings,
                expectedSettings,
                actual,
                expected,
                ignoringSort
            ).match();
            if (match == false) {
                throw new NotEqualMatcherException(
                    actualMappings,
                    actualSettings,
                    expectedMappings,
                    expectedSettings,
                    "actual [" + actual + "] not equal to [" + expected + "]"
                );
            }
        }

        @Override
        public CompareStep<T> ignoringSort(boolean ignoringSort) {
            this.ignoringSort = ignoringSort;
            return this;
        }

        @Override
        public CompareStep<T> expected(T expected) {
            this.expected = expected;
            return this;
        }
    }

}
