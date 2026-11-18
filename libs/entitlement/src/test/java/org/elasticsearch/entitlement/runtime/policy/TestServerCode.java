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

/**
 * Mock for server code that does not delegate, plus wrong uses of {@link EntitlementDelegation#asCreator()}.
 */
final class TestServerCode {
    private TestServerCode() {}

    static Class<?> callCallerUtility(PolicyCheckerImpl checker) {
        return TestCallerUtility.requestingClass(checker);
    }

    static TestCreatorUtility createCreatorUtility() {
        return new TestCreatorUtility();
    }

    static Class<?> requestingClass(PolicyCheckerImpl checker) {
        return checker.requestingClass(TestServerCode.class);
    }

    static EntitlementDelegation asCreatorFromMethod() {
        return EntitlementDelegation.asCreator();
    }

    static final class StaticCreatorDelegation {
        static final EntitlementDelegation DELEGATION = EntitlementDelegation.asCreator();
    }
}
