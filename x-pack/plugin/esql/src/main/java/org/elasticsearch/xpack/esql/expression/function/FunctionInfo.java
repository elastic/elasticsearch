/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Describes functions.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.CONSTRUCTOR)
public @interface FunctionInfo {
    /**
     * The type(s) this function returns.
     */
    String[] returnType();

    /**
     * Whether this function is a preview (Not ready for production environments) or not.
     */
    boolean preview() default false;

    /**
     * The description of the function rendered in {@code META FUNCTIONS}
     * and the docs. These should be complete sentences.
     */
    String description() default "";

    /**
     * Detailed descriptions of the function rendered in the docs.
     */
    String detailedDescription() default "";

    /**
     * A {@code NOTE} that's added after the {@link #description} in the docs.
     */
    String note() default "";

    /**
     * Extra information rendered at the bottom of the function docs.
     */
    String appendix() default "";

    /**
     * Is this an aggregation (true) or a scalar function (false).
     */
    boolean isAggregation() default false;

    /**
     * Examples of using this function that are rendered in the docs.
     */
    Example[] examples() default {};
}
