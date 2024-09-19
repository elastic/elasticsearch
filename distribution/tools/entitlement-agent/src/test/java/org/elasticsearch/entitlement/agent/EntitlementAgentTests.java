/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.agent;

import org.elasticsearch.entitlement.runtime.api.EntitlementChecks;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutSecurityManager;

/**
 * This is an end-to-end test that runs with the javaagent installed.
 * It should exhaustively test every instrumented method to make sure it passes with the entitlement
 * and fails without it.
 * See {@code build.gradle} for how we set the command line arguments for this test.
 */
@WithoutSecurityManager
public class EntitlementAgentTests extends ESTestCase {

    public void testAgentBooted() {
        assertTrue(EntitlementChecks.isAgentBooted());
    }

}
