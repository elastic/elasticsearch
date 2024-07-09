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

    public static <T> ActualStep<T> with(
        final XContentBuilder expectedMappings,
        final Settings.Builder expectedSettings,
        final XContentBuilder actualMappings,
        final Settings.Builder actualSettings
    ) {
        return new Builder<>(expectedMappings, expectedSettings, actualMappings, actualSettings);
    }

    public interface ActualStep<T> {
        ExpectedStep<T> actual(T actual);
    }

    public interface ExpectedStep<T> {
        IgnoreSortingStep expected(T expected);
    }

    public interface IgnoreSortingStep {
        CompareStep ignoreSorting(boolean ignoreSorting);
    }

    public interface CompareStep {
        void isEqual() throws MatcherException;
    }

    private static class Builder<T> implements ActualStep<T>, ExpectedStep<T>, IgnoreSortingStep, CompareStep {

        protected final XContentBuilder expectedMappings;
        protected final Settings.Builder expectedSettings;
        protected final XContentBuilder actualMappings;
        protected final Settings.Builder actualSettings;
        private T expected;
        private T actual;
        private boolean ignoreSorting;

        @Override
        public ExpectedStep<T> actual(T actual) {
            this.actual = actual;
            return this;
        }

        @Override
        public IgnoreSortingStep expected(T expected) {
            this.expected = expected;
            return this;
        }

        @Override
        public void isEqual() throws MatcherException {
            boolean match = new EqualMatcher<>(
                actualMappings,
                actualSettings,
                expectedMappings,
                expectedSettings,
                actual,
                expected,
                ignoreSorting
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

        private Builder(
            final XContentBuilder expectedMappings,
            final Settings.Builder expectedSettings,
            final XContentBuilder actualMappings,
            final Settings.Builder actualSettings

        ) {
            this.expectedMappings = expectedMappings;
            this.expectedSettings = expectedSettings;
            this.actualMappings = actualMappings;
            this.actualSettings = actualSettings;
        }

        @Override
        public CompareStep ignoreSorting(boolean ignoreSorting) {
            this.ignoreSorting = ignoreSorting;
            return this;
        }
    }

}
