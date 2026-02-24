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

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedDefaultIfDenied = "true")
    static String systemSetIn() {
        var original = System.in;
        System.setIn(new java.io.ByteArrayInputStream(new byte[0]));
        boolean unchanged = System.in == original;
        if (unchanged == false) {
            System.setIn(original);
        }
        return String.valueOf(unchanged);
    }

    @SuppressForbidden(reason = "This should be a no-op so we don't interfere with system streams")
    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedDefaultIfDenied = "true")
    static String systemSetOut() {
        var original = System.out;
        System.setOut(new java.io.PrintStream(java.io.OutputStream.nullOutputStream()));
        boolean unchanged = System.out == original;
        if (unchanged == false) {
            System.setOut(original);
        }
        return String.valueOf(unchanged);
    }

    @SuppressForbidden(reason = "This should be a no-op so we don't interfere with system streams")
    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedDefaultIfDenied = "true")
    static String systemSetErr() {
        var original = System.err;
        System.setErr(new java.io.PrintStream(java.io.OutputStream.nullOutputStream()));
        boolean unchanged = System.err == original;
        if (unchanged == false) {
            System.setErr(original);
        }
        return String.valueOf(unchanged);
    }

    private static final Thread NO_OP_SHUTDOWN_HOOK = new Thread(() -> {}, "Shutdown hook for testing");

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedDefaultIfDenied = "true")
    static String runtimeAddShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {}, "Test shutdown hook"));
        return "true";
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedDefaultIfDenied = "false")
    static String runtimeRemoveShutdownHook() {
        boolean result = Runtime.getRuntime().removeShutdownHook(NO_OP_SHUTDOWN_HOOK);
        return String.valueOf(result);
    }

    private SystemActions() {}
}
