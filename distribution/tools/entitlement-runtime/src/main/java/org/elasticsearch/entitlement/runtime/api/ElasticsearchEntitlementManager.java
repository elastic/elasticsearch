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

import java.util.Optional;

import static org.elasticsearch.entitlement.runtime.internals.EntitlementInternals.isActive;

/**
 * Implementation of the {@link EntitlementChecks} interface, providing additional
 * API methods for managing the checks.
 * The trampoline module loads this object via SPI.
 */
public class ElasticsearchEntitlementManager implements EntitlementChecks {
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
        isActive = true;
    }

    @Override
    public void checkSystemExit(Class<?> callerClass, int status) {
        var requestingModule = requestingModule(callerClass);
        if (isTriviallyAllowed(requestingModule)) {
            // System.out.println(" - Trivially allowed");
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
        int framesToSkip = 1  // getCallingClass (this method)
            + 1  // the checkXxx method
            + 1  // the runtime config method
            + 1  // the instrumented method
        ;
        Optional<Module> module = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
            .walk(
                s -> s.skip(framesToSkip)
                    .map(f -> f.getDeclaringClass().getModule())
                    .filter(m -> m.getLayer() != ModuleLayer.boot())
                    .findFirst()
            );
        return module.orElse(null);
    }

    private static boolean isTriviallyAllowed(Module requestingModule) {
        return isActive == false || (requestingModule == null) || requestingModule == System.class.getModule();
    }

}
