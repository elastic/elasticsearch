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
 * Marks a static method for runtime evaluator generation using ASM bytecode generation.
 * <p>
 * Unlike {@link Evaluator} which has SOURCE retention and triggers compile-time code
 * generation via annotation processing, this annotation has RUNTIME retention and
 * triggers bytecode generation when the function is first used.
 * </p>
 * <p>
 * This annotation is intended for ES|QL functions defined in external modules/plugins
 * that cannot use the compile-time annotation processor.
 * </p>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * public class MyFunction extends UnaryScalarFunction {
 *
 *     @RuntimeEvaluator(extraName = "Double")
 *     public static double process(double fieldVal) {
 *         return someComputation(fieldVal);
 *     }
 *
 *     @RuntimeEvaluator(extraName = "Long")
 *     public static long process(long fieldVal) {
 *         return someComputation(fieldVal);
 *     }
 * }
 * }</pre>
 *
 * @see Evaluator for compile-time generation (SOURCE retention)
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RuntimeEvaluator {
    /**
     * Extra part of the name of the evaluator. Use for disambiguating
     * when there are multiple ways to evaluate a function (e.g., different types).
     * <p>
     * For example, {@code extraName = "Double"} on class {@code Abs2} generates
     * an evaluator class named {@code Abs2DoubleEvaluator}.
     * </p>
     */
    String extraName() default "";

    /**
     * Exceptions thrown by the process method to catch and convert
     * into a warning and turn into a null value.
     */
    Class<? extends Exception>[] warnExceptions() default {};
}
