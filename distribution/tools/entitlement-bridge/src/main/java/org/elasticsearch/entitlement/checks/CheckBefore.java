/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.checks;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Instructs the entitlement {@code Instrumenter} to insert a call to the annotated method at the start
 * of the bytecode for the indicated {@link #method}.
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface CheckBefore {
    /**
     * @return The name of the method in which to insert the call.
     */
    String method();
}
