/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import java.util.List;
import java.util.regex.Pattern;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.junit.Assert.assertTrue;

/**
 * How should we assert the warnings returned by ESQL.
 */
public interface AssertWarnings {
    void assertWarnings(List<String> warnings);

    record NoWarnings() implements AssertWarnings {
        @Override
        public void assertWarnings(List<String> warnings) {
            assertMap(warnings.stream().sorted().toList(), matchesList());
        }
    }

    record ExactStrings(List<String> expected) implements AssertWarnings {
        @Override
        public void assertWarnings(List<String> warnings) {
            assertMap(warnings.stream().sorted().toList(), matchesList(expected.stream().sorted().toList()));
        }
    }

    record DeduplicatedStrings(List<String> expected) implements AssertWarnings {
        @Override
        public void assertWarnings(List<String> warnings) {
            assertMap(warnings.stream().sorted().distinct().toList(), matchesList(expected.stream().sorted().toList()));
        }
    }

    record AllowedRegexes(List<Pattern> expected) implements AssertWarnings {
        @Override
        public void assertWarnings(List<String> warnings) {
            for (String warning : warnings) {
                assertTrue("Unexpected warning: " + warning, expected.stream().anyMatch(x -> x.matcher(warning).matches()));
            }
        }
    }
}
