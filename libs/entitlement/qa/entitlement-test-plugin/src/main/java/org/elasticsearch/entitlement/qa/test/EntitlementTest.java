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
     * When a denied entitlement strategy returns a default value instead of throwing,
     * this specifies the expected string representation of that default value. Test methods
     * using this must return a {@code String} representing the actual result. The test
     * infrastructure will verify the returned value matches this when running in a denied context.
     * An empty string (the default) means no default value check is performed.
     */
    String expectedDefaultIfDenied() default "";

    int fromJavaVersion() default -1;
}
