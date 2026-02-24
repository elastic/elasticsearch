/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.ann;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Runtime-retained version of {@link Fixed} for use with {@link RuntimeEvaluator}.
 * <p>
 * Marks parameters on methods annotated with {@link RuntimeEvaluator} to indicate
 * parameters that are provided to the generated evaluator's constructor rather
 * than recalculated for every row.
 * </p>
 * <p>
 * Unlike {@link Fixed} which has SOURCE retention for compile-time code generation,
 * this annotation has RUNTIME retention for runtime bytecode generation.
 * </p>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * @RuntimeEvaluator(extraName = "")
 * public static BytesRef process(
 *     BytesRef str,
 *     @RuntimeFixed int length
 * ) {
 *     // length is computed once in the factory, not per-row
 *     return str.substring(0, length);
 * }
 * }</pre>
 *
 * @see Fixed for compile-time generation (SOURCE retention)
 * @see RuntimeEvaluator
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface RuntimeFixed {
    /**
     * Should this attribute be in the Evaluator's {@code toString}?
     */
    boolean includeInToString() default true;

    /**
     * Defines the scope of the parameter.
     * <ul>
     *   <li>SINGLETON (default) - Same value for all evaluators, shared across threads</li>
     *   <li>THREAD_LOCAL - Computed per DriverContext (not yet supported in runtime generation)</li>
     * </ul>
     */
    Scope scope() default Scope.SINGLETON;

    /**
     * Defines the parameter scope
     */
    enum Scope {
        /**
         * Should be used for immutable parameters that can be shared across different threads
         */
        SINGLETON,
        /**
         * Should be used for mutable or not thread safe parameters.
         * Note: THREAD_LOCAL is not yet supported in runtime generation.
         */
        THREAD_LOCAL,
    }
}
