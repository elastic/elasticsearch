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
     * Defines the scope of the parameter.
     * - SINGLETON (default) will build a single instance and share it across all evaluators
     * - THREAD_LOCAL will build a new instance for each evaluator thread
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
         * Should be used for mutable or not thread safe parameters
         */
        THREAD_LOCAL,
    }
}
