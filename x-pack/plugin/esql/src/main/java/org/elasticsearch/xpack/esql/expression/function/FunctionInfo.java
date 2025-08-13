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
     * If this function implements an operator, what is its symbol?
     * <p>
     *     This exists entirely to add to the Kibana function definition
     *     json files. Kibana thinks of something as an operator if the
     *     text that triggers it is not the name of the function. So {@code +}
     *     is an operator but {@code IS NULL} doesn't count.
     * </p>
     */
    String operator() default "";

    /**
     * The type(s) this function returns.
     */
    String[] returnType();

    /**
     * Whether this function is a preview (Not ready for production environments) or not.
     */
    boolean preview() default false;

    /**
     * Whether this function applies to particular versions of Elasticsearch.
     */
    FunctionAppliesTo[] appliesTo() default {};

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
     * Adjusts documentation heading level (0=standard, 1=subheading, etc).
     * Used to create logical nesting between related functions.
     */
    int depthOffset() default 0;

    /**
     * The position the function can appear in the language.
     */
    FunctionType type() default FunctionType.SCALAR;

    /**
     * Examples of using this function that are rendered in the docs.
     */
    Example[] examples() default {};
}
