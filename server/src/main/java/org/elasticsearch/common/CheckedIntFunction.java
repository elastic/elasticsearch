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
 * A functional interface that represents a function accepting an int-valued argument,
 * producing a result, and may throw a checked exception during execution.
 * This is the int-consuming primitive specialization for {@link CheckedBiFunction}.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * CheckedIntFunction<String, IOException> indexToName = (index) -> {
 *     if (index < 0) {
 *         throw new IOException("Invalid index: " + index);
 *     }
 *     return "Item-" + index;
 * };
 *
 * try {
 *     String name = indexToName.apply(5);
 * } catch (IOException e) {
 *     // Handle exception
 * }
 * }</pre>
 *
 * @param <T> the type of the result
 * @param <E> the type of exception that may be thrown
 */
@FunctionalInterface
public interface CheckedIntFunction<T, E extends Exception> {
    /**
     * Applies this function to the given int-valued argument.
     *
     * @param input the input value
     * @return the function result
     * @throws E if an error occurs during execution
     */
    T apply(int input) throws E;
}
