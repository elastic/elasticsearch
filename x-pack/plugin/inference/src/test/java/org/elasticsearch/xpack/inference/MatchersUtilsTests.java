/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.SelfDescribing;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.is;

public class MatchersUtilsTests extends ESTestCase {

    public void testIsEqualIgnoreWhitespaceInJsonString_Pattern() {
        var json = """

            {
              "key": "value"
            }

            """;

        Pattern pattern = MatchersUtils.IsEqualIgnoreWhitespaceInJsonString.WHITESPACE_IN_JSON_EXCEPT_KEYS_AND_VALUES_PATTERN;
        Matcher matcher = pattern.matcher(json);
        String jsonWithRemovedWhitespaces = matcher.replaceAll("");

        assertThat(jsonWithRemovedWhitespaces, is("""
            {"key":"value"}"""));
    }

    public void testIsEqualIgnoreWhitespaceInJsonString_Pattern_DoesNotRemoveWhitespaceInKeysAndValues() {
        var json = """

            {
              "key 1": "value 1"
            }

            """;

        Pattern pattern = MatchersUtils.IsEqualIgnoreWhitespaceInJsonString.WHITESPACE_IN_JSON_EXCEPT_KEYS_AND_VALUES_PATTERN;
        Matcher matcher = pattern.matcher(json);
        String jsonWithRemovedWhitespaces = matcher.replaceAll("");

        assertThat(jsonWithRemovedWhitespaces, is("""
            {"key 1":"value 1"}"""));
    }

    public void testIsEqualIgnoreWhitespaceInJsonString_MatchesSafely_DoesMatch() {
        var json = """

            {
              "key 1": "value 1",
              "key 2: {
                "key 3: "value 3"
              },
              "key 4": [
                "value 4", "value 5"
              ]
            }

            """;

        var jsonWithDifferentSpacing = """
            {"key 1": "value 1",
              "key 2:    {
                "key 3:      "value 3"
              },
                 "key 4":    [
                "value 4",     "value 5"
              ]
            }

            """;

        var typeSafeMatcher = new MatchersUtils.IsEqualIgnoreWhitespaceInJsonString(json);
        boolean matches = typeSafeMatcher.matchesSafely(jsonWithDifferentSpacing);

        assertTrue(matches);
    }

    public void testIsEqualIgnoreWhitespaceInJsonString_MatchesSafely_DoesNotMatch() {
        var json = """

            {
              "key 1": "value 1",
              "key 2: {
                "key 3: "value 3"
              },
              "key 4": [
                "value 4", "value 5"
              ]
            }

            """;

        // one value missing in array
        var jsonWithDifferentSpacing = """
            {"key 1": "value 1",
              "key 2:    {
                "key 3:      "value 3"
              },
                 "key 4":    [
                "value 4"
              ]
            }

            """;

        var typeSafeMatcher = new MatchersUtils.IsEqualIgnoreWhitespaceInJsonString(json);
        boolean matches = typeSafeMatcher.matchesSafely(jsonWithDifferentSpacing);

        assertFalse(matches);
    }

    public void testIsEqualIgnoreWhitespaceInJsonString_DescribeTo() {
        var jsonOne = """
            {
              "key": "value"
            }
            """;

        var typeSafeMatcher = new MatchersUtils.IsEqualIgnoreWhitespaceInJsonString(jsonOne);
        var description = new TestDescription("");

        typeSafeMatcher.describeTo(description);

        assertThat(description.toString(), is("""
            a string equal to (when all whitespaces are ignored expect in keys and values): {"key":"value"}"""));
    }

    private static class TestDescription implements Description {

        private String descriptionContent;

        TestDescription(String descriptionContent) {
            Objects.requireNonNull(descriptionContent);
            this.descriptionContent = descriptionContent;
        }

        @Override
        public Description appendText(String text) {
            descriptionContent += text;
            return this;
        }

        @Override
        public Description appendDescriptionOf(SelfDescribing value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Description appendValue(Object value) {
            descriptionContent += value;
            return this;
        }

        @SafeVarargs
        @Override
        public final <T> Description appendValueList(String start, String separator, String end, T... values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Description appendValueList(String start, String separator, String end, Iterable<T> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Description appendList(String start, String separator, String end, Iterable<? extends SelfDescribing> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return descriptionContent;
        }
    }
}
