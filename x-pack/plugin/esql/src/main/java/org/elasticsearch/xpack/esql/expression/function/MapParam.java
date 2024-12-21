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
 * Describes function parameters.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface MapParam {
    String name();

    // AbstractFunctionTestCase validate if the type match the one used in the testcase.
    // map is not a supported data type in ES|QL yet, mark it as unsupported both here and in the AbstractFunctionTestCase.
    String[] type() default "map";

    MapEntry[] paramHint() default {};

    String description() default "";

    boolean optional() default false;

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    @interface MapEntry {
        String key() default "";

        String[] value() default {};
    }
}
