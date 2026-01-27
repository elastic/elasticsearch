/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.entitled;

import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.concurrent.atomic.AtomicBoolean;

public class EntitledPlugin extends Plugin implements ExtensiblePlugin {

    /**
     * Runs some actions that should be allowed or denied for this plugin,
     * to ensure the entitlement system is handling them correctly.
     */
    public static void selfTest() {
        selfTestEntitled();
        selfTestNotEntitled();
    }

    private static void selfTestEntitled() {
        logger.debug("selfTestEntitled");
        AtomicBoolean threadRan = new AtomicBoolean(false);
        try {
            Thread testThread = new Thread(() -> threadRan.set(true), "testThread");
            testThread.start();
            testThread.join();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        if (threadRan.get() == false) {
            throw new AssertionError("Self-test thread did not run");
        }
    }

    private static void selfTestNotEntitled() {
        logger.debug("selfTestNotEntitled");
        try {
            System.setIn(System.in);
        } catch (NotEntitledException e) {
            // All is well
            return;
        }
        throw new AssertionError("Expected self-test not to be entitled");
    }

    private static final Logger logger = LogManager.getLogger(EntitledPlugin.class);
}
