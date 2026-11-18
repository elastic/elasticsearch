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
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;

import java.util.function.Supplier;

/**
 * Mock for server code that delegates to its caller.
 */
final class TestCallerUtility {
    private static final EntitlementDelegation DELEGATION = EntitlementDelegation.asCaller();

    private TestCallerUtility() {}

    static Class<?> requestingClass(PolicyCheckerImpl checker) {
        return DELEGATION.call(() -> helperRequestingClass(checker));
    }

    static Class<?> requestingClassWithInlineDelegation(PolicyCheckerImpl checker) {
        return EntitlementDelegation.asCaller().call(() -> helperRequestingClass(checker));
    }

    static Class<?> nestedRequestingClass(PolicyCheckerImpl checker) {
        return DELEGATION.call(() -> Nested.requestingClass(checker));
    }

    static Class<?> requestingClassThroughOtherMethod(PolicyCheckerImpl checker) {
        return requestingClass(checker);
    }

    static Class<?> requestingClassOutsideCall(PolicyCheckerImpl checker) {
        return helperRequestingClass(checker);
    }

    static Supplier<Class<?>> requestingClassSupplier(PolicyCheckerImpl checker) {
        return new RequestingClass(checker);
    }

    static void delegatedCheck(PolicyCheckerImpl checker) {
        DELEGATION.call(() -> {
            checker.checkEntitlementPresent(TestCallerUtility.class, ExitVMEntitlement.class);
            return null;
        });
    }

    static Class<?> failingRequestingClass(PolicyCheckerImpl checker) throws Exception {
        return DELEGATION.call(() -> { throw new Exception("failed in [" + helperRequestingClass(checker).getName() + "]"); });
    }

    static <T> T withCallback(Supplier<T> callback) {
        return DELEGATION.call(callback::get);
    }

    static TestCreatorUtility createCreatorUtility() {
        return DELEGATION.call(TestCreatorUtility::new);
    }

    private static Class<?> helperRequestingClass(PolicyCheckerImpl checker) {
        return checker.requestingClass(TestCallerUtility.class);
    }

    static final class Nested {
        static Class<?> requestingClass(PolicyCheckerImpl checker) {
            return checker.requestingClass(Nested.class);
        }
    }

    private record RequestingClass(PolicyCheckerImpl checker) implements Supplier<Class<?>> {
        @Override
        public Class<?> get() {
            return DELEGATION.call(() -> checker.requestingClass(RequestingClass.class));
        }
    }
}
