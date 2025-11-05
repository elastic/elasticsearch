/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

/**
 * A generic tuple containing two values.
 *
 * <p>This record provides a simple container for holding two related values of
 * potentially different types. It is immutable and provides standard equals,
 * hashCode, and toString implementations.
 *
 * @param <V1> the type of the first value
 * @param <V2> the type of the second value
 * @param v1 the first value
 * @param v2 the second value
 *
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * Tuple<String, Integer> userInfo = new Tuple<>("Alice", 25);
 * String name = userInfo.v1();
 * Integer age = userInfo.v2();
 *
 * // Or using the factory method
 * Tuple<String, Integer> userInfo2 = Tuple.tuple("Bob", 30);
 * }</pre>
 */
public record Tuple<V1, V2>(V1 v1, V2 v2) {

    /**
     * Creates a new tuple with the specified values.
     *
     * <p>This is a convenience factory method that can be statically imported
     * for more concise tuple creation.
     *
     * @param <V1> the type of the first value
     * @param <V2> the type of the second value
     * @param v1 the first value
     * @param v2 the second value
     * @return a new Tuple containing the specified values
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * import static org.elasticsearch.core.Tuple.tuple;
     *
     * Tuple<String, Integer> pair = tuple("key", 42);
     * }</pre>
     */
    public static <V1, V2> Tuple<V1, V2> tuple(V1 v1, V2 v2) {
        return new Tuple<>(v1, v2);
    }
}
