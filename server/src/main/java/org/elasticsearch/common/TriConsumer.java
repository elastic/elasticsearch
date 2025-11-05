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
 * Represents an operation that accepts three arguments and returns no result.
 * This is a three-arity specialization of {@link java.util.function.Consumer}.
 * Unlike most other functional interfaces, {@code TriConsumer} is expected to operate via side-effects.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * TriConsumer<String, Integer, List<String>> listPopulator = (prefix, count, list) -> {
 *     for (int i = 0; i < count; i++) {
 *         list.add(prefix + i);
 *     }
 * };
 *
 * List<String> items = new ArrayList<>();
 * listPopulator.apply("Item-", 5, items);
 * // items now contains: ["Item-0", "Item-1", "Item-2", "Item-3", "Item-4"]
 * }</pre>
 *
 * @param <S> the type of the first argument
 * @param <T> the type of the second argument
 * @param <U> the type of the third argument
 */
@FunctionalInterface
public interface TriConsumer<S, T, U> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param s the first input argument
     * @param t the second input argument
     * @param u the third input argument
     */
    void apply(S s, T t, U u);
}
