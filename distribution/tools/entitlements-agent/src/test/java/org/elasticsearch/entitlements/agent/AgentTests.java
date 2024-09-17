/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlements.agent;

import org.elasticsearch.entitlements.runtime.api.EntitlementChecks;
import org.elasticsearch.entitlements.runtime.api.NotEntitledException;
import org.elasticsearch.entitlements.runtime.internals.EntitlementInternals;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutSecurityManager;
import org.junit.After;

/**
 * This is an end-to-end test that runs with the javaagent installed.
 * It should exhaustively test every instrumented method to make sure it passes with the entitlement
 * and fails without it.
 * See {@code build.gradle} for how we set the command line arguments for this test.
 */
@WithoutSecurityManager
public class AgentTests extends ESTestCase {

    @After
    public void deactivate() {
        // Without this, JUnit can't exit
        EntitlementInternals.isActive = false;
    }

    /**
     * We can't really check that this one passes because it will just exit the JVM.
     */
    public void test_exitNotEntitled_throws() {
        EntitlementChecks.activate();
        assertThrows(NotEntitledException.class, () -> System.exit(123));
    }

}
