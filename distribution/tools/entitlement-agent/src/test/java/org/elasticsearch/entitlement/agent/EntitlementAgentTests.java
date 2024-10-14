/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.agent;

import com.carrotsearch.randomizedtesting.annotations.SuppressForbidden;

import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementManager;
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.entitlement.runtime.internals.EntitlementInternals;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutSecurityManager;
import org.junit.After;

/**
 * This is an end-to-end test of the agent and entitlement runtime.
 * It runs with the agent installed, and exhaustively tests every instrumented method
 * to make sure it works with the entitlement granted and throws without it.
 * The only exception is {@link System#exit}, where we can't that it works without
 * terminating the JVM.
 * <p>
 * If you're trying to debug the instrumentation code, take a look at {@code InstrumenterTests}.
 * That tests the bytecode portion without firing up an agent, which makes everything easier to troubleshoot.
 * <p>
 * See {@code build.gradle} for how we set the command line arguments for this test.
 */
@WithoutSecurityManager
public class EntitlementAgentTests extends ESTestCase {

    public static final ElasticsearchEntitlementManager ENTITLEMENT_MANAGER = ElasticsearchEntitlementManager.get();

    @After
    public void resetEverything() {
        EntitlementInternals.reset();
    }

    /**
     * We can't really check that this one passes because it will just exit the JVM.
     */
    @SuppressForbidden("Specifically testing System.exit")
    public void testSystemExitNotEntitled() {
        ENTITLEMENT_MANAGER.activate();
        assertThrows(NotEntitledException.class, () -> System.exit(123));
    }

}
