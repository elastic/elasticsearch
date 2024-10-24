/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.entitlement.api.EntitlementChecks;
import org.elasticsearch.entitlement.api.EntitlementProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Optional;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;
import static org.elasticsearch.entitlement.runtime.internals.EntitlementInternals.isActive;

/**
 * Implementation of the {@link EntitlementChecks} interface, providing additional
 * API methods for managing the checks.
 * The bridge module loads this object via SPI.
 */
public class ElasticsearchEntitlementManager implements EntitlementChecks {
    public ElasticsearchEntitlementManager() {
        logger.info("Entitlement manager started - inactive");
    }

    /**
     * @return the same instance of {@link ElasticsearchEntitlementManager} returned by {@link EntitlementProvider}.
     */
    public static ElasticsearchEntitlementManager get() {
        return (ElasticsearchEntitlementManager) EntitlementProvider.checks();
    }

    /**
     * Causes entitlements to be enforced.
     */
    public void activate() {
        logger.info("Activating entitlement checks");
        isActive = true;
    }

    @Override
    public void checkSystemExit(Class<?> callerClass, int status) {
        var requestingModule = requestingModule(callerClass);
        if (isTriviallyAllowed(requestingModule)) {
            return;
        }
        // Hard-forbidden until we develop the permission granting scheme
        throw new NotEntitledException("Missing entitlement for " + requestingModule);
    }

    private static Module requestingModule(Class<?> callerClass) {
        if (callerClass != null) {
            Module callerModule = callerClass.getModule();
            if (callerModule.getLayer() != ModuleLayer.boot()) {
                // fast path
                return callerModule;
            }
        }
        int framesToSkip = 1  // requestingModule (this method)
            + 1  // the checkXxx method
            + 1  // the instrumented method
        ;
        Optional<Module> module = StackWalker.getInstance(RETAIN_CLASS_REFERENCE)
            .walk(
                s -> s.skip(framesToSkip)
                    .map(f -> f.getDeclaringClass().getModule())
                    .filter(m -> m.getLayer() != ModuleLayer.boot())
                    .findFirst()
            );
        return module.orElse(null);
    }

    private static boolean isTriviallyAllowed(Module requestingModule) {
        if (isActive == false) {
            logger.trace("Entitlements are inactive");
            return true;
        }
        if (requestingModule == null) {
            logger.trace("Entire call stack is in the boot module layer");
            return true;
        }
        if (requestingModule == System.class.getModule()) {
            logger.trace("Caller is java.base");
            return true;
        }
        logger.trace("Not trivially allowed");
        return false;
    }

    private static final Logger logger = LogManager.getLogger(ElasticsearchEntitlementManager.class);

}
