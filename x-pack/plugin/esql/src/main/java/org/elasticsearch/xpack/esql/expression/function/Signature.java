/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * One concrete overload of a function: argument types plus return type.
 * <p>
 *     A parameter entry may use {@code |} for a within-signature union and/or
 *     {@link TypeGroup} names ({@code NUMERIC}, {@code STRING}, {@code GEO},
 *     {@code SORTABLE}, {@code ALL}), e.g. {@code "NUMERIC|keyword"} or
 *     {@code "date|STRING"}. Unions and groups are expanded when validating
 *     tests and generating docs.
 * </p>
 * <p>
 *     {@link #returnType()} is either a single concrete type name, a positional
 *     reference {@code $N} (exact type of parameter {@code N}), or {@code $N.noText}
 *     (same, after {@code DataType.noText()}). Type groups and {@code |} unions are
 *     not allowed in the return type.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Signature {
    /**
     * Argument types for this overload. Optional trailing parameters are omitted
     * rather than listed as nullable. Use {@code |} for a union and/or a
     * {@link TypeGroup} name in one position.
     */
    String[] params();

    /**
     * The return type of this overload: a concrete type name, {@code $N} to follow
     * parameter {@code N} exactly, or {@code $N.noText} to follow it after
     * {@code DataType.noText()}.
     */
    String returnType();
}
