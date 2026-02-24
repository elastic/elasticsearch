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
 * Marks a static method for runtime MvEvaluator generation using bytecode generation.
 * <p>
 * This annotation is the runtime equivalent of {@link MvEvaluator} which has SOURCE
 * retention and triggers compile-time code generation. This annotation has RUNTIME
 * retention and triggers bytecode generation when the function is first used.
 * </p>
 * <p>
 * MvEvaluators reduce multivalued fields into a single value. The annotated method
 * can use different processing modes:
 * </p>
 * <ul>
 *   <li><b>Pairwise processing</b>: {@code process(T current, T next)} - simple accumulation</li>
 *   <li><b>Accumulator processing</b>: {@code process(State state, T v)} with {@link #finish()} - for complex state</li>
 * </ul>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * public class MvSum2 extends EsqlScalarFunction {
 *
 *     @RuntimeMvEvaluator(extraName = "Int", warnExceptions = {ArithmeticException.class})
 *     public static int processInt(int current, int next) {
 *         return Math.addExact(current, next);
 *     }
 *
 *     @RuntimeMvEvaluator(extraName = "Double", single = "singleDouble", ascending = "ascendingDouble")
 *     public static double processDouble(double current, double next) {
 *         return current + next;
 *     }
 * }
 * }</pre>
 *
 * @see MvEvaluator for compile-time generation (SOURCE retention)
 * @see RuntimeEvaluator for regular (non-MV) runtime evaluator generation
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RuntimeMvEvaluator {
    /**
     * Extra part of the name of the evaluator. Use for disambiguating
     * when there are multiple ways to evaluate a function (e.g., different types).
     * <p>
     * For example, {@code extraName = "Int"} on class {@code MvSum2} generates
     * an evaluator class named {@code MvSum2IntEvaluator}.
     * </p>
     */
    String extraName() default "";

    /**
     * Optional method called to convert state into result.
     * Required when the process method uses accumulator processing (workType != resultType).
     * <p>
     * The finish method signature should be: {@code ResultType finish(WorkType work, int valueCount)}
     * where valueCount is optional.
     * </p>
     */
    String finish() default "";

    /**
     * Optional method called to process single valued fields.
     * If specified, blocks containing only single valued fields will call this method
     * instead of the process method.
     * <p>
     * The single method signature should be: {@code ResultType single(FieldType value)}
     * </p>
     */
    String single() default "";

    /**
     * Optional method called to process blocks whose values are sorted in ascending order.
     * This is an optimization for data loaded from Lucene which is often sorted.
     * The ascending method can have different signatures:
     * <ul>
     *   <li>Index lookup: {@code int ascending(int valueCount)} - returns index to fetch</li>
     *   <li>Block mode: {@code ResultType ascending(Block v, int first, int valueCount)}</li>
     *   <li>Block mode with work: {@code ResultType ascending(WorkType work, Block v, int first, int valueCount)}</li>
     * </ul>
     */
    String ascending() default "";

    /**
     * Exceptions thrown by the process method to catch and convert
     * into a warning and turn into a null value.
     */
    Class<? extends Exception>[] warnExceptions() default {};
}
