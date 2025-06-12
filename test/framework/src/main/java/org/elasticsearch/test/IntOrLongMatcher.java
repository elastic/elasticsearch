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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

/**
 * A type-agnostic way of comparing integer values, not caring if it's a long or an integer.
 */
public abstract sealed class IntOrLongMatcher<T> extends BaseMatcher<T> {
    public static IntOrLongMatcher<Integer> matches(int expected) {
        return new IntMatcher(expected);
    }

    public static IntOrLongMatcher<Long> matches(long expected) {
        return new LongMatcher(expected);
    }

    private static final class IntMatcher extends IntOrLongMatcher<Integer> {
        private final int expected;

        private IntMatcher(int expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(Object o) {
            return switch (o) {
                case Integer i -> expected == i;
                case Long l -> expected == l;
                default -> false;
            };
        }

        @Override
        public void describeTo(Description description) {
            equalTo(expected).describeTo(description);
        }
    }

    private static final class LongMatcher extends IntOrLongMatcher<Long> {
        private final long expected;

        LongMatcher(long expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(Object o) {
            return switch (o) {
                case Integer i -> expected == i;
                case Long l -> expected == l;
                default -> false;
            };
        }

        @Override
        public void describeTo(Description description) {
            equalTo(expected).describeTo(description);
        }
    }

    public static Matcher<Object> isIntOrLong() {
        return anyOf(isA(Integer.class), isA(Long.class));
    }
}
