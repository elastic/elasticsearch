/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.test.LambdaMatchers.everyItemMatches;
import static org.elasticsearch.test.LambdaMatchers.matches;
import static org.elasticsearch.test.LambdaMatchers.transformed;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class LambdaMatchersTests extends ESTestCase {

    private static class A {
        final String str;

        private A(String str) {
            this.str = str;
        }

        @Override
        public String toString() {
            return "A[" + str + "]";
        }
    }

    private static class B extends A {
        private B(String str) {
            super(str);
        }

        @Override
        public String toString() {
            return "B[" + str + "]";
        }
    }

    private static class ToStringPredicate<T> implements Predicate<T> {
        private final Predicate<T> predicate;
        private final String toString;

        private ToStringPredicate(Predicate<T> predicate, String toString) {
            this.predicate = predicate;
            this.toString = toString;
        }

        @Override
        public boolean test(T t) {
            return predicate.test(t);
        }

        @Override
        public String toString() {
            return toString;
        }
    }

    public void testPredicateMatcher() {
        assertThat(new A(""), matches(a -> a.str.isEmpty()));
        assertThat(new B(""), matches((A a) -> a.str.isEmpty()));   // check the types all fit together

        assertMismatch(new A("x"), matches(a -> a.str.isEmpty(), "MyPredicate"), startsWith("<A[x]> did not match predicate MyPredicate"));
    }

    public void testPredicateDescription() {
        assertDescribeTo(matches(o -> true, "MyPredicate"), equalTo("matches predicate MyPredicate"));
        assertDescribeTo(matches(new ToStringPredicate<>(o -> true, "MyPredicate")), equalTo("matches predicate <MyPredicate>"));
    }

    public void testCollectionPredicate() {
        // just to check the types all work with everyItem
        Collection<A> as = List.of(new A("1"), new A("2"), new A("3"));
        assertThat(as, everyItem(matches(a -> a.str.isEmpty() == false)));
    }

    public void testArrayPredicate() {
        A[] as = new A[] { new A("1"), new A("2"), new A("3") };
        assertThat(as, everyItemMatches(a -> a.str.isEmpty() == false));

        assertMismatch(
            as,
            everyItemMatches(a -> a.str.isEmpty(), "empty string"),
            equalTo("an item <A[1]> did not match predicate empty string")
        );
    }

    public void testArrayDescription() {
        assertDescribeTo(everyItemMatches((A a) -> a.str.isEmpty(), "empty string"), equalTo("every item matches predicate empty string"));
    }

    public void testTransformMatcher() {
        assertThat(new A("1"), transformed(a -> a.str, equalTo("1")));
        assertThat(new B("1"), transformed((A a) -> a.str, equalTo("1")));

        assertMismatch(new A("1"), transformed(a -> a.str, emptyString()), equalTo("transformed value was \"1\""));
    }

    public void testTransformDescription() {
        assertDescribeTo(transformed((A a) -> a.str, emptyString()), equalTo("transformed to match an empty string"));
    }

    static <T> void assertMismatch(T v, Matcher<? super T> matcher, Matcher<String> mismatchDescriptionMatcher) {
        assertThat(v, not(matcher));
        StringDescription description = new StringDescription();
        matcher.describeMismatch(v, description);
        assertThat(description.toString(), mismatchDescriptionMatcher);
    }

    static void assertDescribeTo(Matcher<?> matcher, Matcher<String> describeToMatcher) {
        StringDescription description = new StringDescription();
        matcher.describeTo(description);
        assertThat(description.toString(), describeToMatcher);
    }
}
