/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

/**
 * Represents a function that accepts four arguments and produces a result.
 *
 * @param <S> the type of the first argument
 * @param <T> the type of the second argument
 * @param <U> the type of the third argument
 * @param <V> the type of the fourth argument
 * @param <R> the return type
 */
@FunctionalInterface
public interface QuadFunction<S, T, U, V, R> {
    /**
     * Applies this function to the given arguments.
     *
     * @param s the first function argument
     * @param t the second function argument
     * @param u the third function argument
     * @param v the fourth function argument
     * @return the result
     */
    R apply(S s, T t, U u, V v);
}
