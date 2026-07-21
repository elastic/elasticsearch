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
 * Implement an evaluator for a function that applies a lambda expression to every element
 * of a multivalued field, from a static {@code process} method that combines the lambda's
 * per-element results back into one output position per input position.
 * <p>
 *     The generated evaluator owns everything except the combine step: it evaluates the
 *     field expression, flattens multivalued positions into one row per value (a null or
 *     empty position becomes a single null row), builds the lambda body's input page
 *     (row-replicating only the upstream blocks the body actually references, with the
 *     flattened field appended as the lambda parameter channel), evaluates the lambda body
 *     once, vectorized, over that page, and then calls the annotated method once per
 *     original position with the half-open row range {@code [start, end)} that corresponds
 *     to that position's values.
 * </p>
 * <p>
 *     Annotated methods can have two shapes. Functions whose result is computed from the
 *     lambda's output alone use:
 * </p>
 * <pre>{@code
 *     static void process(IntBlock.Builder builder, IntBlock body, int start, int end)
 * }</pre>
 * <p>
 *     Functions that also need the field's own values (e.g. a filter returning the matching
 *     elements) take the flattened field block as well:
 * </p>
 * <pre>{@code
 *     static void process(IntBlock.Builder builder, IntBlock field, BooleanBlock body, int start, int end)
 * }</pre>
 * <p>
 *     The builder's type determines the evaluator's result type and the {@code body} block's
 *     type must match the lambda body's output type. Within {@code [start, end)} the
 *     {@code field} block rows are single-valued and non-null (null field positions never
 *     reach the method: the generated code appends {@code null} for them directly), while
 *     {@code body} rows may be null or multivalued — the method decides what those mean.
 *     The method must append <strong>exactly one</strong> position to {@code builder} per
 *     invocation.
 * </p>
 * <p>
 *     The generated evaluator's {@code Factory} takes the field evaluator factory, the lambda
 *     body factory and the {@code int[]} recipe mapping the body's upstream references to
 *     channels of the enclosing page — obtained from {@code ToEvaluator#lambdaBody} in the
 *     esql plugin.
 * </p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface LambdaEvaluator {
    /**
     * Extra part of the name of the evaluator written after the name of the declaring class,
     * used to disambiguate per-type {@code process} overloads (e.g. {@code "Int"}).
     */
    String extraName() default "";
}
