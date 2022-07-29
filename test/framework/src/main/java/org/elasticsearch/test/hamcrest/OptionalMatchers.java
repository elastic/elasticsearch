/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.hamcrest;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Optional;

public class OptionalMatchers {

    private static class IsEmptyMatcher extends TypeSafeMatcher<Optional<?>> {

        @Override
        protected boolean matchesSafely(final Optional<?> item) {
            // noinspection OptionalAssignedToNull
            return item != null && item.isEmpty();
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("expected empty optional");
        }

        @Override
        protected void describeMismatchSafely(final Optional<?> item, final Description mismatchDescription) {
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

    private static class IsPresentMatcher extends TypeSafeMatcher<Optional<?>> {

        @Override
        protected boolean matchesSafely(final Optional<?> item) {
            return item != null && item.isPresent();
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("expected non-empty optional");
        }

        @Override
        protected void describeMismatchSafely(final Optional<?> item, final Description mismatchDescription) {
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

}
