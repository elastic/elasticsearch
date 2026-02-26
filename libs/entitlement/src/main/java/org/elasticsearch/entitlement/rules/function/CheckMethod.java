/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules.function;

import org.elasticsearch.entitlement.runtime.policy.PolicyChecker;

/**
 * Functional interface for performing entitlement checks.
 * <p>
 * A check method receives the calling class and a policy checker, and performs
 * entitlement validation. If the check fails, it may throw an exception.
 * <p>
 * Multiple check methods can be composed using the {@link #and(CheckMethod)} combinator.
 */
public interface CheckMethod {
    /**
     * Performs an entitlement check.
     *
     * @param callingClass the class that is attempting the protected operation
     * @param policyChecker the policy checker to use for validation
     * @throws Exception if the entitlement check fails or encounters an error
     */
    void check(Class<?> callingClass, PolicyChecker policyChecker) throws Exception;

    /**
     * Combines this check method with another, creating a composite check that
     * executes both checks in sequence.
     *
     * @param next the check method to execute after this one
     * @return a composite check method that executes both checks
     */
    default CheckMethod and(CheckMethod next) {
        return (callingClass, policyChecker) -> {
            check(callingClass, policyChecker);
            next.check(callingClass, policyChecker);
        };
    }
}
