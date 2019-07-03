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

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Optional;

public class OptionalMatchers {

    private static class IsEmptyMatcher extends TypeSafeMatcher<Optional<?>> {

        @Override
        protected boolean matchesSafely(final Optional<?> item) {
            // noinspection OptionalAssignedToNull
            return item != null && item.isEmpty();
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("expected empty optional");
        }

        @Override
        protected void describeMismatchSafely(final Optional<?> item, final Description mismatchDescription) {
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText("was ").appendText(item.toString());
            }
        }

    }

    public static IsEmptyMatcher isEmpty() {
        return new IsEmptyMatcher();
    }

    private static class IsPresentMatcher extends TypeSafeMatcher<Optional<?>> {

        @Override
        protected boolean matchesSafely(final Optional<?> item) {
            return item != null && item.isPresent();
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("expected non-empty optional");
        }

        @Override
        protected void describeMismatchSafely(final Optional<?> item, final Description mismatchDescription) {
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText("was empty");
            }
        }

    }

    public static IsPresentMatcher isPresent() {
        return new IsPresentMatcher();
    }

}
