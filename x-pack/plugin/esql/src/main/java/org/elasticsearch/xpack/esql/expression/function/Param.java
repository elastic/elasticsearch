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
        }

        ENTITY_TYPE entityType() default ENTITY_TYPE.NONE;

        Kind kind() default Kind.STANDARD;

        Constraint[] constraints() default {};

        @Retention(RetentionPolicy.RUNTIME)
        @Target(ElementType.PARAMETER)
        @interface Constraint {
            String name();

            String value();
        }
    }
}
