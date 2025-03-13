/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.hamcrest;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Optional;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;

public class OptionalMatchers {

    private static class IsEmptyMatcher extends TypeSafeMatcher<Optional<?>> {
        @Override
        protected boolean matchesSafely(Optional<?> item) {
            return item.isEmpty();
        }

        @Override
        protected void describeMismatchSafely(Optional<?> item, Description mismatchDescription) {
            mismatchDescription.appendText("a non-empty optional ").appendValue(item.get());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("an empty optional");
        }
    }

    public static Matcher<Optional<?>> isEmpty() {
        return new IsEmptyMatcher();
    }

    private static class IsPresentMatcher<T> extends BaseMatcher<Optional<? extends T>> {
        private final Matcher<? super T> contents;

        private IsPresentMatcher(Matcher<? super T> contents) {
            this.contents = contents;
        }

        @Override
        public boolean matches(Object actual) {
            Optional<?> opt = (Optional<?>) actual;
            return opt.isPresent() && contents.matches(opt.get());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a non-empty optional ").appendDescriptionOf(contents);
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            Optional<?> opt = (Optional<?>) item;
            if (opt.isEmpty()) {
                description.appendText("an empty optional");
                return;
            }

            description.appendText("an optional ");
            contents.describeMismatch(opt.get(), description);
        }
    }

    public static Matcher<Optional<?>> isPresent() {
        return new IsPresentMatcher<>(anything());
    }

    public static <T> Matcher<Optional<? extends T>> isPresentWith(T contents) {
        return new IsPresentMatcher<>(equalTo(contents));
    }

    public static <T> Matcher<Optional<? extends T>> isPresentWith(Matcher<? super T> contents) {
        return new IsPresentMatcher<>(contents);
    }
}
