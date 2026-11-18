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
 * Mock for server code that delegates to the class that created the instance.
 */
class TestCreatorUtility {
    private final EntitlementDelegation delegation = EntitlementDelegation.asCreator();

    TestCreatorUtility() {}

    TestCreatorUtility(String unused) {
        this();
    }

    TestCreatorUtility(int unused) {}

    Class<?> requestingClass(PolicyCheckerImpl checker) {
        return delegation.call(() -> checker.requestingClass(TestCreatorUtility.class));
    }

    <T> T withCallback(Supplier<T> callback) {
        return delegation.call(callback::get);
    }

    Class<?> requestingClassOutsideCall(PolicyCheckerImpl checker) {
        return checker.requestingClass(TestCreatorUtility.class);
    }
}
