/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logstashbridge;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class DucktypeMatchers {
    static QuacksLikeMatcher<Class<?>> instancesQuackLike(final Class<?> ducktypeInterface) {
        return new QuacksLikeMatcher<>(ducktypeInterface, Function.identity(), false);
    }

    static QuacksLikeMatcher<Class<?>> staticallyQuacksLike(final Class<?> ducktypeInterface) {
        return new QuacksLikeMatcher<>(ducktypeInterface, Function.identity(), true);
    }

    static <T> QuacksLikeMatcher<T> quacksLike(final Class<?> ducktypeInterface) {
        return new QuacksLikeMatcher<>(ducktypeInterface, T::getClass, false);
    }

    private static class QuacksLikeMatcher<T> extends TypeSafeMatcher<T> {
        private final Class<?> ducktypeInterface;
        private final Function<T, Class<?>> getClass;
        private final boolean matchStaticMethods;

        QuacksLikeMatcher(Class<?> ducktypeInterface, Function<T, Class<?>> getClass, boolean matchStaticMethods) {
            assert ducktypeInterface.isInterface() : ducktypeInterface + " is not an interface";
            assert ducktypeInterface.getInterfaces().length == 0 : ducktypeInterface + " must not extend other interfaces";
            this.ducktypeInterface = ducktypeInterface;
            this.getClass = getClass;
            this.matchStaticMethods = matchStaticMethods;
        }

        @Override
        protected boolean matchesSafely(T candidate) {
            return findMissingMethods(getClass.apply(candidate)).isEmpty();
        }

        @Override
        public void describeTo(Description description) {
            if (matchStaticMethods) {
                description.appendText("statically ");
            }
            description.appendText("quack like ");
            description.appendText(ducktypeInterface.getName());
        }

        @Override
        protected void describeMismatchSafely(T candidate, Description mismatchDescription) {
            mismatchDescription.appendValueList("missing implementations (", ", ", ")", findMissingMethods(getClass.apply(candidate)));
        }

        private Set<Method> findMissingMethods(Class<?> candidate) {
            final Set<Method> missingMethods = new HashSet<>();
            for (Method method : ducktypeInterface.getMethods()) {
                try {
                    Method candidateMethod = candidate.getMethod(method.getName(), method.getParameterTypes());
                    if (Modifier.isStatic(candidateMethod.getModifiers()) != matchStaticMethods) {
                        missingMethods.add(method);
                    }
                } catch (NoSuchMethodException e) {
                    missingMethods.add(method);
                }
            }
            return Set.copyOf(missingMethods);
        }
    }
}
