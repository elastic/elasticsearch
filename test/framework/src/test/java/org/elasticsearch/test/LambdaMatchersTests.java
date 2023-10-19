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

import java.util.List;

import static org.elasticsearch.test.LambdaMatchers.transformedArrayItemsMatch;
import static org.elasticsearch.test.LambdaMatchers.transformedItemsMatch;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

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

    public void testTransformMatcher() {
        assertThat(new A("1"), transformedMatch(a -> a.str, equalTo("1")));
        assertThat(new B("1"), transformedMatch((A a) -> a.str, equalTo("1")));

        assertMismatch(new A("1"), transformedMatch(a -> a.str, emptyString()), equalTo("transformed value was \"1\""));
    }

    public void testTransformDescription() {
        assertDescribeTo(transformedMatch((A a) -> a.str, emptyString()), equalTo("transformed to match an empty string"));
    }

    public void testListTransformMatcher() {
        List<A> as = List.of(new A("1"), new A("2"), new A("3"));
        assertThat(as, transformedItemsMatch(a -> a.str, containsInAnyOrder("1", "2", "3")));
        assertThat(as, transformedItemsMatch(a -> a.str, containsInAnyOrder("1", "2", "3")));

        assertMismatch(
            as,
            transformedItemsMatch(a -> a.str, containsInAnyOrder("1", "2", "4")),
            equalTo("transformed item not matched: \"3\"")
        );
    }

    public void testListTransformDescription() {
        assertDescribeTo(
            transformedItemsMatch((A a) -> a.str, containsInAnyOrder("1")),
            equalTo("iterable with transformed items to match iterable with items [\"1\"] in any order")
        );
    }

    public void testArrayTransformMatcher() {
        A[] as = new A[] { new A("1"), new A("2"), new A("3") };
        assertThat(as, transformedArrayItemsMatch(a -> a.str, arrayContaining("1", "2", "3")));

        assertMismatch(
            as,
            transformedArrayItemsMatch(a -> a.str, arrayContainingInAnyOrder("1", "2", "4")),
            equalTo("transformed item not matched: \"3\"")
        );
    }

    public void testArrayTransformDescription() {
        assertDescribeTo(
            transformedArrayItemsMatch((A a) -> a.str, arrayContainingInAnyOrder("1")),
            equalTo("array with transformed items to match [\"1\"] in any order")
        );
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
