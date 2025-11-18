/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests.NO_ENTITLEMENTS_MODULE;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests.TEST_PATH_LOOKUP;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests.makeClassInItsOwnModule;

public class PolicyCheckerImplTests extends ESTestCase {
    public void testRequestingClassFastPath() throws IOException, ClassNotFoundException {
        var callerClass = makeClassInItsOwnModule();
        assertEquals(callerClass, checker(NO_ENTITLEMENTS_MODULE).requestingClass(callerClass));
    }

    public void testRequestingModuleWithStackWalk() throws IOException, ClassNotFoundException {
        var entitlementsClass = makeClassInItsOwnModule();    // A class in the entitlements library itself
        var instrumentedClass = makeClassInItsOwnModule();    // The class that called the check method
        var requestingClass = makeClassInItsOwnModule();      // This guy is always the right answer
        var ignorableClass = makeClassInItsOwnModule();

        var checker = checker(entitlementsClass.getModule());

        assertEquals(
            "Skip entitlement library and the instrumented method",
            requestingClass,
            checker.findRequestingFrame(
                Stream.of(entitlementsClass, instrumentedClass, requestingClass, ignorableClass).map(PolicyManagerTests.MockFrame::new)
            ).map(StackWalker.StackFrame::getDeclaringClass).orElse(null)
        );
        assertEquals(
            "Skip multiple library frames",
            requestingClass,
            checker.findRequestingFrame(
                Stream.of(entitlementsClass, entitlementsClass, instrumentedClass, requestingClass).map(PolicyManagerTests.MockFrame::new)
            ).map(StackWalker.StackFrame::getDeclaringClass).orElse(null)
        );
        assertThrows(
            "Non-modular caller frames are not supported",
            NullPointerException.class,
            () -> checker.findRequestingFrame(Stream.of(entitlementsClass, null).map(PolicyManagerTests.MockFrame::new))
        );
    }

    private static PolicyCheckerImpl checker(Module entitlementsModule) {
        return new PolicyCheckerImpl(Set.of(), entitlementsModule, null, TEST_PATH_LOOKUP);
    }

}
