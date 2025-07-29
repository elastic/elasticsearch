/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.SuppressForbidden;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.SERVER_ONLY;

@SuppressWarnings({ "unused" /* called via reflection */ })
class SystemActions {

    @SuppressForbidden(reason = "Specifically testing Runtime.exit")
    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void runtimeExit() {
        Runtime.getRuntime().exit(123);
    }

    @SuppressForbidden(reason = "Specifically testing Runtime.halt")
    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void runtimeHalt() {
        Runtime.getRuntime().halt(123);
    }

    @SuppressForbidden(reason = "Specifically testing System.exit")
    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void systemExit() {
        System.exit(123);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void systemSetIn() {
        System.setIn(System.in);
    }

    @SuppressForbidden(reason = "This should be a no-op so we don't interfere with system streams")
    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void systemSetOut() {
        System.setOut(System.out);
    }

    @SuppressForbidden(reason = "This should be a no-op so we don't interfere with system streams")
    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void systemSetErr() {
        System.setErr(System.err);
    }

    private static final Thread NO_OP_SHUTDOWN_HOOK = new Thread(() -> {}, "Shutdown hook for testing");

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void runtimeAddShutdownHook() {
        Runtime.getRuntime().addShutdownHook(NO_OP_SHUTDOWN_HOOK);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void runtimeRemoveShutdownHook() {
        Runtime.getRuntime().removeShutdownHook(NO_OP_SHUTDOWN_HOOK);
    }

    private SystemActions() {}
}
