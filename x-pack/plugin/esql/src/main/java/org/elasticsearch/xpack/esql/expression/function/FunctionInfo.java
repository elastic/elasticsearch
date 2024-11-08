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
     * The description of the function rendered in the docs and kibana's
     * json files that drive their IDE-like experience. These should be
     * complete sentences but can contain asciidoc syntax. It is rendered
     * as a single paragraph.
     */
    String description() default "";

    /**
     * Detailed descriptions of the function rendered in the docs. This is
     * rendered as a single paragraph following {@link #description()} in
     * the docs and is <strong>excluded</strong> from Kibana's IDE-like
     * experience. It can contain asciidoc syntax.
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
