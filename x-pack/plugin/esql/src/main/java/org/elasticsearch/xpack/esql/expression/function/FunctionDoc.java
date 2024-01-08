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

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface FunctionDoc {
    enum FunctionType {
        AGGREGATE,
        DATE_TIME,
        MATH,
        STRING,
    }

    FunctionType type();

    String description();

    String synopsis();

    String[] arguments() default {};

    String output();

    String examples();

}
