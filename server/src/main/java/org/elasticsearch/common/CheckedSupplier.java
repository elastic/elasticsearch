/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.util.function.Supplier;

/**
 * A {@link Supplier}-like interface which allows throwing checked exceptions.
 * This functional interface represents a supplier of results that may throw a checked exception
 * during execution.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * CheckedSupplier<String, IOException> fileReader = () -> {
 *     return Files.readString(Path.of("config.txt"));
 * };
 *
 * try {
 *     String content = fileReader.get();
 * } catch (IOException e) {
 *     // Handle exception
 * }
 * }</pre>
 *
 * @param <R> the type of results supplied by this supplier
 * @param <E> the type of exception that may be thrown
 */
@FunctionalInterface
public interface CheckedSupplier<R, E extends Exception> {
    /**
     * Gets a result.
     *
     * @return a result
     * @throws E if an error occurs during execution
     */
    R get() throws E;
}
