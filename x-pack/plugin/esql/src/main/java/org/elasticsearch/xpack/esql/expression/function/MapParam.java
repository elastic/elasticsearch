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
 * Describes a function map argument represented by {@code MapExpression}.
 * This is added to support search options used by {@code FullTextFunction}, and can be invoked like:
 * FUNCTION(a, b, {"stringArg":"value", "intArg":1, "doubleArg":2.0, "boolArg":true, "arrayArg":["a", "b"]})
 *
 * Using {@code Match} function as an example, the {@code Match} function takes two required arguments,
 * field name and query text, it creates a {@code MatchQuery} under the cover, and the rest of the {@code MatchQuery}
 * options are be grouped together and provided to the {@code Match} function as an optional argument in a map format.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface MapParam {
    String name();

    MapParamEntry[] params() default {};

    String description() default "";

    boolean optional() default false;

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    @interface MapParamEntry {
        String name() default "";

        // A list of valid values/hints of this parameter, it can be a numeric, boolean, string value or an array of these values.
        String[] valueHint() default {};

        String[] type() default {};

        String description() default "";
    }
}
