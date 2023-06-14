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
 * Implement an evaluator for a function applying a static {@code process}
 * method to each value of a multivalued field.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface ConvertEvaluator {
    /**
     * Extra part of the name of the evaluator. Use for disambiguating
     * when there are multiple ways to evaluate a function.
     */
    String extraName() default "";

}
