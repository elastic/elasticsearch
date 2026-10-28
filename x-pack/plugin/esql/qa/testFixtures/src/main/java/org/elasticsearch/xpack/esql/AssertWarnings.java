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
    void assertWarnings(List<String> warnings, Object context);

    default String contextMessage(Object context) {
        if (context == null) {
            return "";
        }
        StringBuilder contextBuilder = new StringBuilder();
        contextBuilder.append("Context: ").append(context);
        if (contextBuilder.length() > 1000) {
            contextBuilder.setLength(1000);
            contextBuilder.append("...(truncated)");
        }
        contextBuilder.append("\n");
        return contextBuilder.toString();
    }

    record NoWarnings() implements AssertWarnings {
        @Override
        public void assertWarnings(List<String> warnings, Object context) {
            assertMap(contextMessage(context), warnings.stream().sorted().toList(), matchesList());
        }
    }

    record ExactStrings(List<String> expected) implements AssertWarnings {
        @Override
        public void assertWarnings(List<String> warnings, Object context) {
            assertMap(contextMessage(context), warnings.stream().sorted().toList(), matchesList(expected.stream().sorted().toList()));
        }
    }

    record DeduplicatedStrings(List<String> expected) implements AssertWarnings {
        @Override
        public void assertWarnings(List<String> warnings, Object context) {
            assertMap(
                contextMessage(context),
                warnings.stream().sorted().distinct().toList(),
                matchesList(expected.stream().sorted().toList())
            );
        }
    }

    record AllowedRegexes(List<Pattern> expected) implements AssertWarnings {
        @Override
        public void assertWarnings(List<String> warnings, Object context) {
            for (String warning : warnings) {
                assertTrue(
                    contextMessage(context) + "Unexpected warning: " + warning,
                    expected.stream().anyMatch(x -> x.matcher(warning).matches())
                );
            }
        }
    }
}
