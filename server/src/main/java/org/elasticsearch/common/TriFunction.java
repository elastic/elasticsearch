/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 *
 */

package org.elasticsearch.common;

/**
 * Represents a function that accepts three arguments and produces a result.
 * This is a three-arity specialization of {@link java.util.function.Function}.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * TriFunction<String, Integer, Boolean, String> formatter = (text, count, uppercase) -> {
 *     String result = text.repeat(count);
 *     return uppercase ? result.toUpperCase() : result;
 * };
 *
 * String output = formatter.apply("Hello", 3, true);
 * // Result: "HELLOHELLOHELLO"
 * }</pre>
 *
 * @param <S> the type of the first argument
 * @param <T> the type of the second argument
 * @param <U> the type of the third argument
 * @param <R> the return type
 */
@FunctionalInterface
public interface TriFunction<S, T, U, R> {
    /**
     * Applies this function to the given arguments.
     *
     * @param s the first function argument
     * @param t the second function argument
     * @param u the third function argument
     * @return the result of applying this function
     */
    R apply(S s, T t, U u);
}
