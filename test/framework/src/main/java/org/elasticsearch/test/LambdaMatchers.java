/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class LambdaMatchers {

    private static class TransformMatcher<T, U> extends BaseMatcher<T> {
        private final Matcher<U> matcher;
        private final Function<T, U> transform;

        private TransformMatcher(Matcher<U> matcher, Function<T, U> transform) {
            this.matcher = matcher;
            this.transform = transform;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean matches(Object actual) {
            U u;
            try {
                u = transform.apply((T) actual);
            } catch (ClassCastException e) {
                throw new AssertionError(e);
            }

            return matcher.matches(u);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void describeMismatch(Object item, Description description) {
            U u;
            try {
                u = transform.apply((T) item);
            } catch (ClassCastException e) {
                description.appendValue(item).appendText(" is not of the correct type (").appendText(e.getMessage()).appendText(")");
                return;
            }

            description.appendText("transformed value ");
            matcher.describeMismatch(u, description);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("transformed to match ").appendDescriptionOf(matcher);
        }
    }

    public static <T, U> Matcher<T> transformedMatch(Function<T, U> function, Matcher<U> matcher) {
        return new TransformMatcher<>(matcher, function);
    }

    private static class ListTransformMatcher<T, U> extends TypeSafeMatcher<Iterable<T>> {
        private final Matcher<Iterable<? extends U>> matcher;
        private final Function<T, U> transform;

        private ListTransformMatcher(Matcher<Iterable<? extends U>> matcher, Function<T, U> transform) {
            this.matcher = matcher;
            this.transform = transform;
        }

        @Override
        protected boolean matchesSafely(Iterable<T> item) {
            List<U> us = new ArrayList<>();
            for (T i : item) {
                try {
                    us.add(transform.apply(i)); // this might not actually be a T
                } catch (ClassCastException e) {
                    throw new AssertionError(e);
                }
            }

            return matcher.matches(us);
        }

        @Override
        protected void describeMismatchSafely(Iterable<T> item, Description description) {
            List<U> us = new ArrayList<>();
            int i = 0;
            for (Iterator<T> iterator = item.iterator(); iterator.hasNext(); i++) {
                try {
                    us.add(transform.apply(iterator.next())); // this might not actually be a T
                } catch (ClassCastException e) {
                    description.appendValue(i)
                        .appendText(" at index " + i)
                        .appendText(" is not of the correct type (")
                        .appendText(e.getMessage())
                        .appendText(")");
                    return;
                }
            }

            description.appendText("transformed item ");
            matcher.describeMismatch(us, description);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("iterable with transformed items to match ").appendDescriptionOf(matcher);
        }
    }

    public static <T, U> Matcher<Iterable<T>> transformedItemsMatch(Function<T, U> function, Matcher<Iterable<? extends U>> matcher) {
        return new ListTransformMatcher<>(matcher, function);
    }

    private static class ArrayTransformMatcher<T, U> extends TypeSafeMatcher<T[]> {
        private final Matcher<U[]> matcher;
        private final Function<T, U> transform;

        private ArrayTransformMatcher(Matcher<U[]> matcher, Function<T, U> transform) {
            this.matcher = matcher;
            this.transform = transform;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean matchesSafely(T[] item) {
            U[] us = null;
            for (int i = 0; i < item.length; i++) {
                U u;
                try {
                    u = transform.apply(item[i]);   // this might not actually be a T
                } catch (ClassCastException e) {
                    throw new AssertionError(e);
                }
                if (us == null) {
                    // now we actually have a U, we can create an array of the correct type
                    us = (U[]) Array.newInstance(u.getClass(), item.length);
                }
                us[i] = u;
            }

            return matcher.matches(us);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void describeMismatchSafely(T[] item, Description description) {
            U[] us = null;
            for (int i = 0; i < item.length; i++) {
                U u;
                try {
                    u = transform.apply(item[i]);   // this might not actually be a T
                } catch (ClassCastException e) {
                    description.appendValue(i)
                        .appendText(" at index " + i)
                        .appendText(" is not of the correct type (")
                        .appendText(e.getMessage())
                        .appendText(")");
                    return;
                }
                if (us == null) {
                    // now we actually have a U, we can create an array of the correct type
                    us = (U[]) Array.newInstance(u.getClass(), item.length);
                }
                us[i] = u;
            }

            description.appendText("transformed item ");
            matcher.describeMismatch(us, description);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("array with transformed items to match ").appendDescriptionOf(matcher);
        }
    }

    public static <T, U> Matcher<T[]> transformedArrayItemsMatch(Function<T, U> function, Matcher<U[]> matcher) {
        return new ArrayTransformMatcher<>(matcher, function);
    }
}
