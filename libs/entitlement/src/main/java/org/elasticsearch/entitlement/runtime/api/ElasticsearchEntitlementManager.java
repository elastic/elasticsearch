/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.entitlement.bridge.EntitlementChecks;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Optional;

import static org.elasticsearch.entitlement.runtime.internals.EntitlementInternals.isActive;

/**
 * Implementation of the {@link EntitlementChecks} interface, providing additional
 * API methods for managing the checks.
 * The trampoline module loads this object via SPI.
 */
public class ElasticsearchEntitlementManager implements EntitlementChecks {
    /**
     * Log level recommendations:
     * <dl>
     *     <dt>{@code INFO}</dt>
     *     <dd>Summary events that typically happen once per Elasticsearch run, such as "agent loaded" or "policies initialized"</dd>
     *     <dt>{@code DEBUG}</dt>
     *     <dd>
     *         <ul>
     *             <li>
     *                 Events that happen once per "decision", such as permitting an operation to proceed
     *             </li>
     *             <li>
     *                 Events that happen once per instrumented method
     *             </li>
     *             <li>
     *                 Finer-grained events that happen once per Elasticsearch run, such as looking for a jar in a particular directory
     *             </li>
     *         </ul>
     *     </dd>
     *     <dt>{@code TRACE}</dt>
     *     <dd>
     *         <ul>
     *             <li>Events that happen in loops</li>
     *             <li>Logs to help comprehend control flow</li>
     *         </ul>
     *     </dd>
     * </dl>
     */
    private static final Logger logger = LogManager.getLogger(ElasticsearchEntitlementManager.class);

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
        if (isActive == false) {
            logger.debug("Trivially allowed: entitlements are inactive");
            return true;
        }
        if (requestingModule == null) {
            logger.debug("Trivially allowed: Entire call stack is in the boot module layer");
            return true;
        }
        if (requestingModule == System.class.getModule()) {
            logger.debug("Trivially allowed: Caller is in {}", System.class.getModule().getName());
            return true;
        }
        logger.trace("Not trivially allowed");
        return false;
    }


}
