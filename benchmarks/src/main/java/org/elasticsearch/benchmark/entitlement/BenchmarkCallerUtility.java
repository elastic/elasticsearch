/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.entitlement;

import org.elasticsearch.entitlement.runtime.api.EntitlementDelegation;
import org.elasticsearch.entitlement.runtime.policy.PolicyChecker;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;

/**
 * Server code that delegates its checks to its caller.
 */
final class BenchmarkCallerUtility {
    private static final EntitlementDelegation DELEGATION = EntitlementDelegation.asCaller();

    private BenchmarkCallerUtility() {}

    static void delegatedCheck(PolicyChecker checker) {
        DELEGATION.call(() -> {
            checker.checkEntitlementPresent(BenchmarkCallerUtility.class, ExitVMEntitlement.class);
            return null;
        });
    }
}
