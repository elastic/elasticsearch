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
import org.elasticsearch.datageneration.matchers.source.SourceMatcher;
import org.elasticsearch.xcontent.XContentBuilder;

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
        SettingsStep<T> mappings(
            Map<String, Map<String, Object>> mappingLookup,
            XContentBuilder actualMappings,
            XContentBuilder expectedMappings
        );
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
        private Map<String, Map<String, Object>> mappingLookup;
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
            final Map<String, Map<String, Object>> mappingLookup,
            final XContentBuilder actualMappings,
            final XContentBuilder expectedMappings
        ) {
            this.mappingLookup = mappingLookup;
            this.actualMappings = actualMappings;
            this.expectedMappings = expectedMappings;

            return this;
        }

        @Override
        public MatchResult isEqualTo(List<Map<String, Object>> actual) {
            return new SourceMatcher(
                mappingLookup,
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
