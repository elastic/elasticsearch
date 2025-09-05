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
 * An example of using a function that is rendered in the docs.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.CONSTRUCTOR)
public @interface Example {

    /**
     * The description that will appear before the example
     */
    String description() default "";

    /**
     * The test file that contains the example.
     */
    String file();

    /**
     * The tag that fences this example.
     */
    String tag();

    /**
     * If the example is applicable to only a capability available in a specific version
     */
    String applies_to() default "";

    /**
     * The explanation that will appear after the example.
     */
    String explanation() default "";
}
