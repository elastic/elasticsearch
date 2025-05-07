/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;

class TestEntitlementBootstrap {

    private static final Logger logger = LogManager.getLogger(TestEntitlementBootstrap.class);

    /**
     * Activates entitlement checking in tests.
     */
    public static void bootstrap() {
        logger.debug("Loading entitlement agent");
        loadAgent(EntitlementBootstrap.findAgentJar());
    }

    @SuppressForbidden(reason = "The VirtualMachine API is the only way to attach a java agent dynamically")
    private static void loadAgent(String agentPath) {
        try {
            VirtualMachine vm = VirtualMachine.attach(Long.toString(ProcessHandle.current().pid()));
            try {
                vm.loadAgent(agentPath, TestEntitlementInitialization.class.getName());
            } finally {
                vm.detach();
            }
        } catch (AttachNotSupportedException | IOException | AgentLoadException | AgentInitializationException e) {
            throw new IllegalStateException("Unable to attach entitlement agent", e);
        }
    }
}
