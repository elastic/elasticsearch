/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.hamcrest;

import org.elasticsearch.common.collect.Tuple;
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
        return new TupleMatcher(v1Matcher, v2Matcher);
    }
}
