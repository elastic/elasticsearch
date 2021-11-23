/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.hamcrest;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.OptionalLong;

public class OptionalLongMatchers {

    private static class IsEmptyMatcher extends TypeSafeMatcher<OptionalLong> {

        @Override
        protected boolean matchesSafely(final OptionalLong item) {
            return item != null && item.isEmpty();
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("expected empty optional");
        }

        @Override
        protected void describeMismatchSafely(final OptionalLong item, final Description mismatchDescription) {
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText("was ").appendText(item.toString());
            }
        }
    }

    public static IsEmptyMatcher isEmpty() {
        return new IsEmptyMatcher();
    }

    private static class IsPresentMatcher extends TypeSafeMatcher<OptionalLong> {

        @Override
        protected boolean matchesSafely(final OptionalLong item) {
            return item != null && item.isPresent();
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("expected non-empty optional");
        }

        @Override
        protected void describeMismatchSafely(final OptionalLong item, final Description mismatchDescription) {
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText("was empty");
            }
        }
    }

    public static IsPresentMatcher isPresent() {
        return new IsPresentMatcher();
    }

    private static class IsPresentWithValueMatcher extends TypeSafeDiagnosingMatcher<OptionalLong> {

        private final Matcher<Long> matcher;

        IsPresentWithValueMatcher(final Matcher<Long> matcher) {
            this.matcher = matcher;
        }

        @Override
        protected boolean matchesSafely(final OptionalLong item, final Description mismatchDescription) {
            if (item == null) {
                mismatchDescription.appendText("was null");
                return false;
            } else if (item.isPresent()) {
                if (matcher.matches(item.getAsLong())) {
                    return true;
                } else {
                    mismatchDescription.appendText("was an OptionalLong whose value ");
                    matcher.describeMismatch(item.getAsLong(), mismatchDescription);
                    return false;
                }
            } else {
                mismatchDescription.appendText("was empty");
                return false;
            }
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("an OptionalLong with ").appendDescriptionOf(matcher);
        }
    }

    public static IsPresentWithValueMatcher optionalWithValue(final Matcher<Long> matcher) {
        return new IsPresentWithValueMatcher(matcher);
    }

}
