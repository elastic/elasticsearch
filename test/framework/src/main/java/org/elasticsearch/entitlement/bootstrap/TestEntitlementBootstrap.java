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
    private static BootstrapArgs bootstrapArgs;

    /**
     * Activates entitlement checking in tests.
     * @param bootstrapArgs arguments used for and passed to entitlement initialization
     */
    public static void bootstrap(BootstrapArgs bootstrapArgs) {
        assert bootstrapArgs != null;
        TestEntitlementBootstrap.bootstrapArgs = bootstrapArgs;
        logger.debug("Loading entitlement agent");
        EntitlementBootstrap.loadAgent(EntitlementBootstrap.findAgentJar(), TestEntitlementInitialization.class.getName());
    }

    public static BootstrapArgs bootstrapArgs() {
        return bootstrapArgs;
    }

    public record BootstrapArgs(PathLookup pathLookup) {}
}
