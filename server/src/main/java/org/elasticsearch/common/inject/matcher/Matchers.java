/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject.matcher;

import java.util.Objects;

/**
 * Matcher implementations. Supports matching classes and methods.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class Matchers {
    private Matchers() {}

    /**
     * Returns a matcher which matches subclasses of the given type (as well as
     * the given type).
     */
    public static Matcher<Class<?>> subclassesOf(final Class<?> superclass) {
        return new SubclassesOf(superclass);
    }

    private static class SubclassesOf extends AbstractMatcher<Class<?>> {
        private final Class<?> superclass;

        SubclassesOf(Class<?> superclass) {
            this.superclass = Objects.requireNonNull(superclass, "superclass");
        }

        @Override
        public boolean matches(Class<?> subclass) {
            return superclass.isAssignableFrom(subclass);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof SubclassesOf && ((SubclassesOf) other).superclass.equals(superclass);
        }

        @Override
        public int hashCode() {
            return 37 * superclass.hashCode();
        }

        @Override
        public String toString() {
            return "subclassesOf(" + superclass.getSimpleName() + ".class)";
        }
    }

    /**
     * Returns a matcher which matches only the given object.
     */
    public static Matcher<Object> identicalTo(final Object value) {
        return new IdenticalTo(value);
    }

    private static class IdenticalTo extends AbstractMatcher<Object> {
        private final Object value;

        IdenticalTo(Object value) {
            this.value = Objects.requireNonNull(value, "value");
        }

        @Override
        public boolean matches(Object other) {
            return value == other;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof IdenticalTo && ((IdenticalTo) other).value == value;
        }

        @Override
        public int hashCode() {
            return 37 * System.identityHashCode(value);
        }

        @Override
        public String toString() {
            return "identicalTo(" + value + ")";
        }
    }

}
