/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.entitlement.bridge.NotEntitledException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EntitlementTest {
    enum ExpectedAccess {
        PLUGINS,
        ES_MODULES_ONLY,
        SERVER_ONLY,
        ALWAYS_DENIED,
        ALWAYS_ALLOWED
    }

    ExpectedAccess expectedAccess();

    Class<? extends Exception> expectedExceptionIfDenied() default NotEntitledException.class;

    /**
     * When a denied entitlement strategy returns a non-null default value instead of throwing,
     * this specifies the expected string representation of that default value as a single-element
     * array. Test methods using this must return the actual result (any type); the infrastructure
     * converts it via {@code toString()}. The test infrastructure will verify the returned value
     * matches the element when running in a denied context.
     * An empty array (the default) means no default value check is performed.
     * Mutually exclusive with {@link #isExpectedDefaultNull()}.
     */
    String[] expectedDefaultIfDenied() default {};

    /**
     * When a denied entitlement strategy returns {@code null} instead of throwing,
     * set this to {@code true}. The test infrastructure will verify that the result
     * is actually {@code null} when running in a denied context.
     * Mutually exclusive with {@link #expectedDefaultIfDenied()} and {@link #isExpectedNoOp()}.
     */
    boolean isExpectedDefaultNull() default false;

    /**
     * When a denied entitlement strategy silently returns early (no-op) for a void method
     * instead of throwing, set this to {@code true}. The test infrastructure will accept
     * a successful response as valid denial behavior.
     * Mutually exclusive with {@link #expectedDefaultIfDenied()} and {@link #isExpectedDefaultNull()}.
     */
    boolean isExpectedNoOp() default false;

    int fromJavaVersion() default -1;
}
