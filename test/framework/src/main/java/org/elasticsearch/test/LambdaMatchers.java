/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.function.Predicate;

public class LambdaMatchers {

    private static class TransformMatcher<T, U> extends TypeSafeMatcher<T> {
        private final String transformDescription;
        private final Matcher<U> matcher;
        private final Function<T, U> transform;

        private TransformMatcher(String transformDescription, Matcher<U> matcher, Function<T, U> transform) {
            this.transformDescription = transformDescription;
            this.matcher = matcher;
            this.transform = transform;
        }

        @Override
        protected boolean matchesSafely(T item) {
            U u;
            try {
                u = transform.apply(item);
            } catch (ClassCastException e) {
                throw new AssertionError(e);
            }
            return matcher.matches(u);
        }

        @Override
        protected void describeMismatchSafely(T item, Description description) {
            U u;
            try {
                u = transform.apply(item);
            } catch (ClassCastException e) {
                description.appendValue(item).appendText(" is not of the correct type (").appendText(e.getMessage()).appendText(")");
                return;
            }

            description.appendText(transformDescription).appendText(" ");
            matcher.describeMismatch(u, description);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(transformDescription).appendText(" matches ").appendDescriptionOf(matcher);
        }
    }

    public static <T, U> Matcher<T> transformedMatch(Function<T, U> function, Matcher<U> matcher) {
        return new TransformMatcher<>("transformed value", matcher, function);
    }

    public static <T, U> Matcher<T> transformedMatch(String description, Function<T, U> function, Matcher<U> matcher) {
        return new TransformMatcher<>(description, matcher, function);
    }

    private static class ListTransformMatcher<T, U> extends TypeSafeMatcher<Iterable<T>> {
        private final String transformDescription;
        private final Matcher<Iterable<? extends U>> matcher;
        private final Function<T, U> transform;

        private ListTransformMatcher(String transformDescription, Matcher<Iterable<? extends U>> matcher, Function<T, U> transform) {
            this.transformDescription = transformDescription;
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

            description.appendText(transformDescription).appendText(" ");
            matcher.describeMismatch(us, description);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("iterable with ").appendText(transformDescription).appendText(" matching ").appendDescriptionOf(matcher);
        }
    }

    public static <T, U> Matcher<Iterable<T>> transformedItemsMatch(Function<T, U> function, Matcher<Iterable<? extends U>> matcher) {
        return new ListTransformMatcher<>("transformed items", matcher, function);
    }

    public static <T, U> Matcher<Iterable<T>> transformedItemsMatch(
        String transformDescription,
        Function<T, U> function,
        Matcher<Iterable<? extends U>> matcher
    ) {
        return new ListTransformMatcher<>(transformDescription, matcher, function);
    }

    private static class ArrayTransformMatcher<T, U> extends TypeSafeMatcher<T[]> {
        private final String transformDescription;
        private final Matcher<U[]> matcher;
        private final Function<T, U> transform;

        private ArrayTransformMatcher(String transformDescription, Matcher<U[]> matcher, Function<T, U> transform) {
            this.transformDescription = transformDescription;
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

            description.appendText(transformDescription).appendText(" ");
            matcher.describeMismatch(us, description);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("array with ").appendText(transformDescription).appendText(" matching ").appendDescriptionOf(matcher);
        }
    }

    public static <T, U> Matcher<T[]> transformedArrayItemsMatch(Function<T, U> function, Matcher<U[]> matcher) {
        return new ArrayTransformMatcher<>("transformed items", matcher, function);
    }

    public static <T, U> Matcher<T[]> transformedArrayItemsMatch(
        String transformDescription,
        Function<T, U> function,
        Matcher<U[]> matcher
    ) {
        return new ArrayTransformMatcher<>(transformDescription, matcher, function);
    }

    private static class PredicateMatcher<T> extends BaseMatcher<Predicate<? super T>> {
        final T item;

        private PredicateMatcher(T item) {
            this.item = item;
        }

        @Override
        @SuppressWarnings({ "rawtypes" })
        public boolean matches(Object actual) {
            Predicate p = (Predicate) actual;
            try {
                return predicateMatches(p);
            } catch (ClassCastException e) {
                return false;
            }
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        protected boolean predicateMatches(Predicate predicate) {
            return predicate.test(item);
        }

        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public void describeMismatch(Object item, Description description) {
            Predicate p = (Predicate) item;
            try {
                boolean result = p.test(this.item);
                description.appendText("predicate with argument ").appendValue(this.item).appendText(" evaluated to ").appendValue(result);
            } catch (ClassCastException e) {
                description.appendText("predicate did not accept argument of type ")
                    .appendValue(this.item.getClass())
                    .appendText(" (")
                    .appendText(e.getMessage())
                    .appendText(")");
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("predicate evaluates to <true> with argument ").appendValue(item);
        }
    }

    public static <T> Matcher<Predicate<? super T>> trueWith(T item) {
        return new PredicateMatcher<>(item);
    }

    private static class PredicateFalseMatcher<T> extends PredicateMatcher<T> {
        private PredicateFalseMatcher(T item) {
            super(item);
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        protected boolean predicateMatches(Predicate predicate) {
            return predicate.test(item) == false;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("predicate evaluates to <false> with argument ").appendValue(item);
        }
    }

    public static <T> Matcher<Predicate<? super T>> falseWith(T item) {
        return new PredicateFalseMatcher<>(item);
    }
}
