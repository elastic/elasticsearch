/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa.matchers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.logsdb.qa.matchers.source.SourceMatcher;

import java.util.List;
import java.util.Map;

/**
 * A base class to be used for the matching logic when comparing query results.
 */
public abstract class Matcher {

    public static <T> SettingsStep<T> mappings(final XContentBuilder actualMappings, final XContentBuilder expectedMappings) {
        return new Builder<>(expectedMappings, actualMappings);
    }

    public static MappingsStep<List<Map<String, Object>>> matchSource() {
        return new SourceMatcherBuilder();
    }

    public interface MappingsStep<T> {
        SettingsStep<T> mappings(XContentBuilder actualMappings, XContentBuilder expectedMappings);
    }

    public interface SettingsStep<T> {
        ExpectedStep<T> settings(Settings.Builder actualSettings, Settings.Builder expectedSettings);
    }

    public interface ExpectedStep<T> {
        CompareStep<T> expected(T expected);
    }

    public interface CompareStep<T> {
        MatchResult isEqualTo(T actual);

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
        public MatchResult isEqualTo(T actual) {
            return new GenericEqualsMatcher<>(
                actualMappings,
                actualSettings,
                expectedMappings,
                expectedSettings,
                actual,
                expected,
                ignoringSort
            ).match();
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

    private static class SourceMatcherBuilder
        implements
            MappingsStep<List<Map<String, Object>>>,
            SettingsStep<List<Map<String, Object>>>,
            CompareStep<List<Map<String, Object>>>,
            ExpectedStep<List<Map<String, Object>>> {
        private XContentBuilder expectedMappings;
        private XContentBuilder actualMappings;
        private Settings.Builder expectedSettings;
        private Settings.Builder actualSettings;
        private List<Map<String, Object>> expected;
        private boolean ignoringSort;

        @Override
        public ExpectedStep<List<Map<String, Object>>> settings(Settings.Builder actualSettings, Settings.Builder expectedSettings) {
            this.actualSettings = actualSettings;
            this.expectedSettings = expectedSettings;
            return this;
        }

        private SourceMatcherBuilder() {}

        public SettingsStep<List<Map<String, Object>>> mappings(
            final XContentBuilder actualMappings,
            final XContentBuilder expectedMappings
        ) {
            this.actualMappings = actualMappings;
            this.expectedMappings = expectedMappings;

            return this;
        }

        @Override
        public MatchResult isEqualTo(List<Map<String, Object>> actual) {
            return new SourceMatcher(actualMappings, actualSettings, expectedMappings, expectedSettings, actual, expected, ignoringSort)
                .match();
        }

        @Override
        public CompareStep<List<Map<String, Object>>> ignoringSort(boolean ignoringSort) {
            this.ignoringSort = ignoringSort;
            return this;
        }

        @Override
        public CompareStep<List<Map<String, Object>>> expected(List<Map<String, Object>> expected) {
            this.expected = expected;
            return this;
        }
    }
}
