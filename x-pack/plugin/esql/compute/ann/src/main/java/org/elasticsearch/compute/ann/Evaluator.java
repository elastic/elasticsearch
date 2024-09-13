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
 * Implement an evaluator from a static {@code process} method. The generated
 * evaluator provides each argument in one of three ways:
 * <ol>
 *     <li>If the argument isn't annotated or an array then it is considered
 *     to be a sub-evaluator and the generated Evaluator will take an
 *     Evaluator for this on construction and call it for each position.</li>
 *     <li>If the argument isn't annotated but is an array then it is considered
 *     to be an array of evaluators and the generated Evaluator will take
 *     an array of Evaluators on construction and evaluate each of them for
 *     each position.</li>
 *     <li>If parameter has the {@link Fixed} annotation then it must be
 *     provided at construction time and is passed unchanged to the process
 *     method.</li>
 * </ol>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface Evaluator {
    /**
     * Extra part of the name of the evaluator. Use for disambiguating
     * when there are multiple ways to evaluate a function.
     */
    String extraName() default "";

    /**
     * Exceptions thrown by the process method to catch and convert
     * into a warning and turn into a null value.
     */
    Class<? extends Exception>[] warnExceptions() default {};
}
