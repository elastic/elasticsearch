/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.api.EntitlementDelegation;

import java.util.function.Supplier;

/**
 * Mock for server code that delegates to its caller, and calls {@link TestCallerUtility}.
 */
final class TestOtherCallerUtility {
    private static final EntitlementDelegation DELEGATION = EntitlementDelegation.asCaller();

    private TestOtherCallerUtility() {}

    static Class<?> callCallerUtility(PolicyCheckerImpl checker) {
        return DELEGATION.call(() -> TestCallerUtility.requestingClass(checker));
    }

    static <T> T call(Supplier<T> operation) {
        return DELEGATION.call(operation::get);
    }
}
