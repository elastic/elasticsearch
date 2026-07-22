/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.core.Nullable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Describes function parameters.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER, ElementType.FIELD })
public @interface Param {
    String name();

    String[] type();

    String description() default "";

    boolean optional() default false;

    String applies_to() default "";

    // version since which the parameter is available
    String since() default "";

    @Nullable
    Hint hint() default @Hint;

    /**
     * Describes the signature of a lambda parameter so tooling (e.g. Kibana autocomplete) can
     * advertise the expected lambda shape to the user.
     * <p>
     * Usage: set {@code paramTypes} to the names of the function's other parameters whose
     * concrete types the lambda parameters inherit (in order), and set {@code returnType} to
     * the fixed return type of the lambda body (e.g. {@code "boolean"} for a predicate).
     * Leave {@code returnType} empty to indicate that the lambda's return type matches the
     * function's own return type (typical for transformation functions like {@code map}).
     * <p>
     * If {@code paramTypes} is empty the annotation is considered absent and no lambda
     * information is emitted.
     */
    Lambda lambda() default @Lambda;

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    @interface Lambda {
        /**
         * Names of the function's other parameters whose concrete types become the lambda's
         * parameter types, in order. Empty means "no lambda info provided".
         */
        String[] paramTypes() default {};

        /**
         * The fixed return type of the lambda body (e.g. {@code "boolean"}).
         * An empty string means the lambda's return type equals the function's return type.
         */
        String returnType() default "";
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    @interface Hint {
        enum ENTITY_TYPE {
            NONE,
            INFERENCE_ENDPOINT,
        }

        enum Kind {
            /**
             * Depends on the function type and command.
             * {@snippet lang="txt" :
             * ┌───────┬──────────────────────────────────────┬──────────────────────────────────┐
             * │       │ Scalar                               │ Aggregation                      │
             * ├───────┼──────────────────────────────────────┼──────────────────────────────────┤
             * │ EVAL  │ may only be a scalar                 │ invalid                          │
             * │ STATS │ must contain an aggregation function │ can only contain scalar function │
             * └───────┴──────────────────────────────────────┴──────────────────────────────────┘
             * }
             */
            STANDARD,
            /**
             * A constant that references some entity to load.
             */
            ENTITY,
            /**
             * This <strong>must</strong> be an aggregation function.
             */
            AGGREGATION,
            /**
             * A constant value.
             */
            CONSTANT
        }

        ENTITY_TYPE entityType() default ENTITY_TYPE.NONE;

        Kind kind() default Kind.STANDARD;

        String[] allowedValues() default {};

        Constraint[] constraints() default {};

        @Retention(RetentionPolicy.RUNTIME)
        @Target(ElementType.PARAMETER)
        @interface Constraint {
            String name();

            String value();
        }
    }
}
