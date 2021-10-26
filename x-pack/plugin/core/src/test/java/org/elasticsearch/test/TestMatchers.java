/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test;

import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class TestMatchers extends Matchers {

    /**
     * @deprecated Use {@link FileMatchers#pathExists}
     */
    @Deprecated
    public static Matcher<Path> pathExists(Path path, LinkOption... options) {
        return new CustomMatcher<Path>("Path " + path + " exists") {
            @Override
            public boolean matches(Object item) {
                return Files.exists(path, options);
            }
        };
    }

    public static Matcher<Throwable> throwableWithMessage(String message) {
        return throwableWithMessage(CoreMatchers.equalTo(message));
    }

    public static Matcher<Throwable> throwableWithMessage(Matcher<String> messageMatcher) {
        return new BaseMatcher<>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("a throwable with message of ").appendDescriptionOf(messageMatcher);
            }

            @Override
            public boolean matches(Object actual) {
                if (actual instanceof Throwable) {
                    final Throwable throwable = (Throwable) actual;
                    return messageMatcher.matches(throwable.getMessage());
                } else {
                    return false;
                }
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                super.describeMismatch(item, description);
                if (item instanceof Throwable) {
                    Throwable e = (Throwable) item;
                    final StackTraceElement at = e.getStackTrace()[0];
                    description.appendText(" at ").appendText(at.toString());
                }
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <T> Matcher<Predicate<T>> predicateMatches(T value) {
        return new CustomMatcher<Predicate<T>>("Matches " + value) {
            @Override
            public boolean matches(Object item) {
                if (Predicate.class.isInstance(item)) {
                    return ((Predicate<T>) item).test(value);
                } else {
                    return false;
                }
            }
        };
    }

    public static Matcher<String> matchesPattern(String regex) {
        return matchesPattern(Pattern.compile(regex));
    }

    public static Matcher<String> matchesPattern(Pattern pattern) {
        return predicate("Matches " + pattern.pattern(), String.class, pattern.asPredicate());
    }

    private static <T> Matcher<T> predicate(String description, Class<T> type, Predicate<T> predicate) {
        return new CustomMatcher<T>(description) {
            @Override
            public boolean matches(Object item) {
                if (type.isInstance(item)) {
                    return predicate.test(type.cast(item));
                } else {
                    return false;
                }
            }
        };
    }

}
