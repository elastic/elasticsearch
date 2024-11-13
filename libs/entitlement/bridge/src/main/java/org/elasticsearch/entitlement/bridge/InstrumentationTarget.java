/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.CLASS;

/**
 * Instructs the entitlement {@code Instrumenter} to insert a call to the annotated method at the start
 * of the bytecode for the method {@link #methodName} in class {@link #className}.
 */
@Target(METHOD)
@Retention(CLASS)
public @interface InstrumentationTarget {
    /**
     * @return The "internal name" of the class in which to insert the check: includes the package info, but with periods replaced
     * by slashes. E.g. java/lang/System
     */
    String className();

    /**
     * @return The name of the method in which to insert the check.
     */
    String methodName();

    /**
     * @return The name of the method in which to insert the check.
     */
    boolean isStatic() default false;
}
