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
 * Runtime trigger for generating {@code AggregatorFunction} implementations.
 * This is the runtime equivalent of {@link Aggregator}, using RUNTIME retention
 * so the annotation is available for reflection-based bytecode generation.
 *
 * <p>The annotated class must provide:
 * <ul>
 *   <li>{@code init} or {@code initSingle} - static method returning initial state (primitive or AggregatorState)</li>
 *   <li>{@code combine} - static method combining state with a new value</li>
 * </ul>
 *
 * <p>For non-primitive state types, also provide:
 * <ul>
 *   <li>{@code combineIntermediate} - combines intermediate states</li>
 *   <li>{@code evaluateFinal} - produces final output Block</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * {@literal @}RuntimeAggregator(
 *     intermediateState = {
 *         {@literal @}RuntimeIntermediateState(name = "sum", type = "LONG"),
 *         {@literal @}RuntimeIntermediateState(name = "seen", type = "BOOLEAN")
 *     }
 * )
 * public class Sum2Aggregator {
 *     public static long init() { return 0; }
 *     public static long combine(long current, int v) { return Math.addExact(current, v); }
 * }
 * </pre>
 *
 * @see Aggregator for compile-time equivalent
 * @see RuntimeIntermediateState for intermediate state description
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RuntimeAggregator {

    /**
     * Description of the intermediate state columns.
     */
    RuntimeIntermediateState[] intermediateState() default {};

    /**
     * Exceptions thrown by the {@code combine*()} methods to catch and convert
     * into a warning and turn into a null value.
     */
    Class<? extends Exception>[] warnExceptions() default {};

    /**
     * Whether to generate a grouping aggregator in addition to the non-grouping one.
     * Defaults to true.
     */
    boolean grouping() default true;
}
