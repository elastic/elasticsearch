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
@Target(ElementType.PARAMETER)
public @interface Param {
    String name();

    String[] type();

    String description() default "";

    boolean optional() default false;

    @Nullable
    AutocompleteHint autocompleteHint() default @AutocompleteHint(entityType = AutocompleteHint.ENTITY_TYPE.NONE);

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    @interface AutocompleteHint {
        enum ENTITY_TYPE {
            NONE,
            INFERENCE_ENDPOINT,
        }

        ENTITY_TYPE entityType();

        Constraint[] constraints() default {};

        @Retention(RetentionPolicy.RUNTIME)
        @Target(ElementType.PARAMETER)
        @interface Constraint {
            String name();

            String value();
        }
    }
}
