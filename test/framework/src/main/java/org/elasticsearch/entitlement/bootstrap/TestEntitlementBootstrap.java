/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import org.elasticsearch.entitlement.initialization.TestEntitlementInitialization;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class TestEntitlementBootstrap {

    private static final Logger logger = LogManager.getLogger(TestEntitlementBootstrap.class);

    /**
     * Activates entitlement checking in tests.
     * @param pathLookup
     */
    public static void bootstrap(PathLookup pathLookup) {
        assert pathLookup != null;
        TestEntitlementInitialization.initializeArgs = new TestEntitlementInitialization.InitializeArgs(pathLookup);
        logger.debug("Loading entitlement agent");
        EntitlementBootstrap.loadAgent(EntitlementBootstrap.findAgentJar(), TestEntitlementInitialization.class.getName());
    }

}
