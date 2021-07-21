/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.hamcrest;

import org.elasticsearch.core.Tuple;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;

public class TupleMatchers {

    private static class TupleMatcher<V1, V2> extends TypeSafeMatcher<Tuple<? extends V1, ? extends V2>> {

        private final Matcher<? super V1> v1Matcher;
        private final Matcher<? super V2> v2Matcher;

        private TupleMatcher(Matcher<? super V1> v1Matcher, Matcher<? super V2> v2Matcher) {
            this.v1Matcher = v1Matcher;
            this.v2Matcher = v2Matcher;
        }

        @Override
        protected boolean matchesSafely(final Tuple<? extends V1, ? extends V2> item) {
            return item != null && v1Matcher.matches(item.v1()) && v2Matcher.matches(item.v2());
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("expected tuple matching ").appendList("[", ", ", "]", Arrays.asList(v1Matcher, v2Matcher));
        }
    }

    /**
     * Creates a matcher that matches iff:
     *  1. the examined tuple's <code>v1()</code> matches the specified <code>v1Matcher</code>
     * and
     *  2. the examined tuple's <code>v2()</code> matches the specified <code>v2Matcher</code>
     * For example:
     * <pre>assertThat(Tuple.tuple("myValue1", "myValue2"), isTuple(startsWith("my"), containsString("Val")))</pre>
     */
    public static <V1, V2> TupleMatcher<? extends V1, ? extends V2> isTuple(Matcher<? super V1> v1Matcher, Matcher<? super V2> v2Matcher) {
        return new TupleMatcher<>(v1Matcher, v2Matcher);
    }
}
