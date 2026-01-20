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

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface DenseVectorEvaluator {
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
