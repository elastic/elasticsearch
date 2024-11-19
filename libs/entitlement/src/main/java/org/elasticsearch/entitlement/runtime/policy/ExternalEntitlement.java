/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation indicates an {@link Entitlement} is available
 * to "external" classes such as those used in plugins. Any {@link Entitlement}
 * using this annotation is considered parseable as part of a policy file
 * for entitlements.
 */
@Target(ElementType.CONSTRUCTOR)
@Retention(RetentionPolicy.RUNTIME)
public @interface ExternalEntitlement {

    /**
     * This is the list of parameter names that are
     * parseable in {@link PolicyParser#parseEntitlement(String, String)}.
     * The number and order of parameter names much match the number and order
     * of constructor parameters as this is how the parser will pass in the
     * parsed values from a policy file. However, the names themselves do NOT
     * have to match the parameter names of the constructor.
     */
    String[] parameterNames() default {};
}
