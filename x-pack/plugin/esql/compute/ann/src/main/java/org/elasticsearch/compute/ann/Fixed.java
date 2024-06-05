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
import java.util.function.Function;

/**
 * Used on parameters on methods annotated with {@link Evaluator} to indicate
 * parameters that are provided to the generated evaluator's constructor rather
 * than recalculated for every row.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.SOURCE)
public @interface Fixed {
    /**
     * Should this attribute be in the Evaluator's {@code toString}?
     */
    boolean includeInToString() default true;

    /**
     * Should the Evaluator's factory build this per evaluator with a
     * {@code Function<DriverContext, T>} or just take fixed implementation?
     * This is typically set to {@code true} to use the {@link Function}
     * to make "scratch" objects which have to be isolated in a single thread.
     * This is typically set to {@code false} when the parameter is simply
     * immutable and can be shared.
     */
    boolean build() default false;
}
