/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.util.function.BiConsumer;

/**
 * A {@link BiConsumer}-like interface which allows throwing checked exceptions.
 * This functional interface represents an operation that accepts two input arguments and returns no result,
 * but may throw a checked exception during execution.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * CheckedBiConsumer<String, OutputStream, IOException> fileWriter = (content, stream) -> {
 *     stream.write(content.getBytes());
 *     stream.flush();
 * };
 *
 * try {
 *     fileWriter.accept("Hello World", outputStream);
 * } catch (IOException e) {
 *     // Handle exception
 * }
 * }</pre>
 *
 * @param <T> the type of the first input parameter
 * @param <U> the type of the second input parameter
 * @param <E> the type of exception that may be thrown
 */
@FunctionalInterface
public interface CheckedBiConsumer<T, U, E extends Exception> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input parameter
     * @param u the second input parameter
     * @throws E if an error occurs during execution
     */
    void accept(T t, U u) throws E;
}
