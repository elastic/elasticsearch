/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.agent;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.entitlement.runtime.checks.EntitlementChecksService;
import org.elasticsearch.entitlement.runtime.internals.EntitlementInternals;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutSecurityManager;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.PrintStream;

import static org.elasticsearch.entitlement.runtime.api.FlagEntitlement.EXIT_JVM;
import static org.elasticsearch.entitlement.runtime.api.FlagEntitlement.SET_SYSTEM_STREAMS;

/**
 * This is an end-to-end test of the agent and entitlement runtime.
 * It runs with the agent installed, and exhaustively tests every instrumented method
 * to make sure it works with the entitlement granted and throws without it.
 * The only exception is {@link System#exit}, where we can't that it works without
 * terminating the JVM.
 * See {@code build.gradle} for how we set the command line arguments for this test.
 */
@WithoutSecurityManager
@SuppressForbidden(reason = "Some of the sensitive functionality we're locking down is in forbidden APIs")
public class EntitlementAgentTests extends ESTestCase {

    public static final EntitlementChecksService CHECKS = EntitlementChecksService.get();

    static InputStream originalIn;
    static PrintStream originalOut, originalErr;

    @BeforeClass
    public static void saveSystemStreams() {
        originalIn = System.in;
        originalOut = System.out;
        originalErr = System.err;
    }

    @After
    public void resetEverything() {
        EntitlementInternals.reset();
        System.setIn(originalIn);
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    public void testFreezeForbidsGrant() {
        CHECKS.activate();
        assertThrows(NotEntitledException.class, () -> CHECKS.checkSystemExit(null, null, 456));

        CHECKS.freeze();

        assertThrows(IllegalStateException.class, () -> CHECKS.grant(getClass().getModule(), EXIT_JVM));
        assertThrows("Grant should not take effect when frozen", NotEntitledException.class, () -> System.exit(123));
    }

    public void testFreezeForbidsRevokeAll() {
        CHECKS.activate();
        CHECKS.grant(getClass().getModule(), EXIT_JVM);
        CHECKS.checkSystemExit(null, null, 456); // Should not throw

        CHECKS.freeze();

        assertThrows(IllegalStateException.class, CHECKS::revokeAll);
        CHECKS.checkSystemExit(null, null, 456); // Revoke should not take effect
    }

    public void testSystemSetIn() {
        var oldIn = System.in;
        var newIn = new BufferedInputStream(oldIn);
        assertNotSame("Sanity check that we can tell the streams apart", oldIn, newIn);

        System.setIn(newIn);
        assertSame("Allowed before activated", newIn, System.in);

        CHECKS.activate();
        assertThrows(NotEntitledException.class, () -> System.setIn(oldIn));

        CHECKS.grant(getClass().getModule(), SET_SYSTEM_STREAMS);
        System.setIn(oldIn);
        assertSame("Allowed after grant", oldIn, System.in);
    }

    public void testSystemSetOut() {
        var oldOut = System.out;
        var newOut = new PrintStream(oldOut);
        assertNotSame("Sanity check that we can tell the streams apart", oldOut, newOut);

        System.setOut(newOut);
        assertSame("Allowed before activated", newOut, System.out);

        CHECKS.activate();
        assertThrows(NotEntitledException.class, () -> System.setOut(oldOut));

        CHECKS.grant(getClass().getModule(), SET_SYSTEM_STREAMS);
        System.setOut(oldOut);
        assertSame("Allowed after grant", oldOut, System.out);
    }

    public void testSystemSetErr() {
        var oldErr = System.err;
        var newErr = new PrintStream(oldErr);
        assertNotSame("Sanity check that we can tell the streams apart", oldErr, newErr);

        System.setErr(newErr);
        assertSame("Allowed before activated", newErr, System.err);

        CHECKS.activate();
        assertThrows(NotEntitledException.class, () -> System.setErr(oldErr));

        CHECKS.grant(getClass().getModule(), SET_SYSTEM_STREAMS);
        System.setErr(oldErr);
        assertSame("Allowed after grant", oldErr, System.err);
    }

    /**
     * We can't really check that this one passes because it will just exit the JVM.
     */
    public void test_exitNotEntitled_throws() {
        CHECKS.activate();
        assertThrows(NotEntitledException.class, () -> System.exit(123));
    }

}
