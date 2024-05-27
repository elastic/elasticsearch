/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.regex.Pattern;

/**
 * Utility class containing custom hamcrest {@link Matcher} implementations or other utility functionality related to hamcrest.
 */
public class MatchersUtils {

    /**
     * Custom matcher implementing a matcher operating on json strings ignoring whitespaces, which are not inside a key or a value.
     *
     * Example:
     * {
     *     "key": "value"
     * }
     *
     * will match
     *
     * {"key":"value"}
     *
     * as both json strings are equal ignoring the whitespace, which does not reside in a key or a value.
     *
     */
    protected static class IsEqualIgnoreWhitespaceInJsonString extends TypeSafeMatcher<String> {

        protected static final Pattern WHITESPACE_IN_JSON_EXCEPT_KEYS_AND_VALUES_PATTERN = createPattern();

        private static Pattern createPattern() {
            String regex = "(?<=[:,\\[{])\\s+|\\s+(?=[\\]}:,])|^\\s+|\\s+$";
            return Pattern.compile(regex);
        }

        private final String string;

        IsEqualIgnoreWhitespaceInJsonString(String string) {
            if (string == null) {
                throw new IllegalArgumentException("Non-null value required");
            }
            this.string = string;
        }

        @Override
        protected boolean matchesSafely(String item) {
            java.util.regex.Matcher itemMatcher = WHITESPACE_IN_JSON_EXCEPT_KEYS_AND_VALUES_PATTERN.matcher(item);
            java.util.regex.Matcher stringMatcher = WHITESPACE_IN_JSON_EXCEPT_KEYS_AND_VALUES_PATTERN.matcher(string);

            String itemReplacedWhitespaces = itemMatcher.replaceAll("");
            String stringReplacedWhitespaces = stringMatcher.replaceAll("");

            return itemReplacedWhitespaces.equals(stringReplacedWhitespaces);
        }

        @Override
        public void describeTo(Description description) {
            java.util.regex.Matcher stringMatcher = WHITESPACE_IN_JSON_EXCEPT_KEYS_AND_VALUES_PATTERN.matcher(string);
            String stringReplacedWhitespaces = stringMatcher.replaceAll("");

            description.appendText("a string equal to (when all whitespaces are ignored expect in keys and values): ")
                .appendValue(stringReplacedWhitespaces);
        }

        public static Matcher<String> equalToIgnoringWhitespaceInJsonString(String expectedString) {
            return new IsEqualIgnoreWhitespaceInJsonString(expectedString);
        }
    }

    public static Matcher<String> equalToIgnoringWhitespaceInJsonString(String expectedString) {
        return IsEqualIgnoreWhitespaceInJsonString.equalToIgnoringWhitespaceInJsonString(expectedString);
    }

}
