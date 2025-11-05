/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

/**
 * A {@link java.util.function.BiFunction}-like interface which allows throwing checked exceptions.
 * This functional interface represents a function that accepts two arguments, produces a result,
 * and may throw a checked exception during execution.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * CheckedBiFunction<String, String, byte[], IOException> contentCombiner = (first, second) -> {
 *     String combined = first + second;
 *     return combined.getBytes("UTF-8");
 * };
 *
 * try {
 *     byte[] result = contentCombiner.apply("Hello", "World");
 * } catch (IOException e) {
 *     // Handle exception
 * }
 * }</pre>
 *
 * @param <T> the type of the first input parameter
 * @param <U> the type of the second input parameter
 * @param <R> the type of the result
 * @param <E> the type of exception that may be thrown
 */
@FunctionalInterface
public interface CheckedBiFunction<T, U, R, E extends Exception> {
    /**
     * Applies this function to the given arguments.
     *
     * @param t the first input parameter
     * @param u the second input parameter
     * @return the function result
     * @throws E if an error occurs during execution
     */
    R apply(T t, U u) throws E;
}
