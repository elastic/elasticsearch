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

import static org.elasticsearch.entitlement.runtime.api.FlagEntitlement.EXIT_JVM;

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

    @After
    public void resetEverything() {
        EntitlementInternals.reset();
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

    /**
     * We can't really check that this one passes because it will just exit the JVM.
     */
    public void test_exitNotEntitled_throws() {
        CHECKS.activate();
        assertThrows(NotEntitledException.class, () -> System.exit(123));
    }

}
