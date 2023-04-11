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
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class LambdaMatchers {

    private static class PredicateMatcher<T> extends TypeSafeDiagnosingMatcher<T> {
        private final Predicate<? super T> predicate;
        private final Consumer<Description> description;

        private PredicateMatcher(Predicate<? super T> predicate, Consumer<Description> description) {
            this.predicate = predicate;
            this.description = description;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("matches predicate ");
            this.description.accept(description);
        }

        @Override
        protected boolean matchesSafely(T item, Description mismatchDescription) {
            boolean result = predicate.test(item);
            if (result == false) {
                mismatchDescription.appendValue(item).appendText(" did not match predicate ");
                this.description.accept(mismatchDescription);
            }
            return result;
        }
    }

    public static <T> Matcher<T> matches(Predicate<? super T> predicate) {
        return new PredicateMatcher<>(predicate, d -> d.appendValue(predicate));
    }

    public static <T> Matcher<T> matches(Predicate<? super T> predicate, String description) {
        return new PredicateMatcher<>(predicate, d -> d.appendText(description));
    }

    public static <T> Matcher<T> matches(Predicate<? super T> predicate, Supplier<String> description) {
        return new PredicateMatcher<>(predicate, d -> d.appendText(description.get()));
    }

    private static class EveryArrayItem<T> extends TypeSafeDiagnosingMatcher<T[]> {
        private final Matcher<T> itemMatcher;

        private EveryArrayItem(Matcher<T> itemMatcher) {
            this.itemMatcher = itemMatcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("every item ").appendDescriptionOf(itemMatcher);
        }

        @Override
        protected boolean matchesSafely(T[] array, Description mismatchDescription) {
            for (T t : array) {
                if (itemMatcher.matches(t) == false) {
                    mismatchDescription.appendText("an item ");
                    itemMatcher.describeMismatch(t, mismatchDescription);
                    return false;
                }
            }
            return true;
        }
    }

    public static <T> Matcher<T[]> everyItemMatches(Predicate<? super T> predicate) {
        return new EveryArrayItem<>(matches(predicate));
    }

    public static <T> Matcher<T[]> everyItemMatches(Predicate<? super T> predicate, String description) {
        return new EveryArrayItem<>(matches(predicate, description));
    }

    public static <T> Matcher<T[]> everyItemMatches(Predicate<? super T> predicate, Supplier<String> description) {
        return new EveryArrayItem<>(matches(predicate, description));
    }

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
                return false;
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

    public static <T, U> Matcher<T> transformed(Function<T, U> function, Matcher<U> matcher) {
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
            List<U> us = transform(item, Description.NONE);
            if (us == null) return false;

            return matcher.matches(us);
        }

        @Override
        protected void describeMismatchSafely(Iterable<T> item, Description description) {
            List<U> us = transform(item, description);
            if (us == null) return;

            description.appendText("transformed item ");
            matcher.describeMismatch(us, description);
        }

        private List<U> transform(Iterable<T> it, Description description) {
            List<U> us = new ArrayList<>();
            int i = 0;
            for (Iterator<T> iterator = it.iterator(); iterator.hasNext(); i++) {
                try {
                    us.add(transform.apply(iterator.next())); // this might not actually be a T
                } catch (ClassCastException e) {
                    description.appendValue(i)
                        .appendText(" at index " + i)
                        .appendText(" is not of the correct type (")
                        .appendText(e.getMessage())
                        .appendText(")");
                    return null;
                }
            }
            return us;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("iterable with transformed items to match ").appendDescriptionOf(matcher);
        }
    }

    public static <T, U> Matcher<Iterable<T>> transformedItems(Function<T, U> function, Matcher<Iterable<? extends U>> matcher) {
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
        protected boolean matchesSafely(T[] item) {
            U[] us = transform(item, Description.NONE);
            if (us == null) return false;

            return matcher.matches(us);
        }

        @Override
        protected void describeMismatchSafely(T[] item, Description description) {
            U[] us = transform(item, description);
            if (us == null) return;

            description.appendText("transformed item ");
            matcher.describeMismatch(us, description);
        }

        @SuppressWarnings("unchecked")
        private U[] transform(T[] array, Description description) {
            U[] us = null;
            for (int i = 0; i < array.length; i++) {
                U u;
                try {
                    u = transform.apply(array[i]);   // this might not actually be a T
                } catch (ClassCastException e) {
                    description.appendValue(i)
                        .appendText(" at index " + i)
                        .appendText(" is not of the correct type (")
                        .appendText(e.getMessage())
                        .appendText(")");
                    return null;
                }
                if (us == null) {
                    // now we actually have a U, we can create an array of the correct type
                    us = (U[]) Array.newInstance(u.getClass(), array.length);
                }
                us[i] = u;
            }
            return us;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("array with transformed items to match ").appendDescriptionOf(matcher);
        }
    }

    public static <T, U> Matcher<T[]> transformedArrayItems(Function<T, U> function, Matcher<U[]> matcher) {
        return new ArrayTransformMatcher<>(matcher, function);
    }
}
