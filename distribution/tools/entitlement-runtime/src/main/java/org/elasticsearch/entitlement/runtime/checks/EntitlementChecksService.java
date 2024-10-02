/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.checks;

import org.elasticsearch.entitlement.checks.EntitlementChecks;
import org.elasticsearch.entitlement.checks.EntitlementProvider;
import org.elasticsearch.entitlement.runtime.api.Entitlement;
import org.elasticsearch.entitlement.runtime.api.FlagEntitlement;
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.newSetFromMap;
import static org.elasticsearch.entitlement.runtime.internals.EntitlementInternals.isActive;
import static org.elasticsearch.entitlement.runtime.internals.EntitlementInternals.isFrozen;

/**
 * Implementation of the {@link EntitlementChecks} interface.
 * The trampoline module loads this object via SPI.
 */
public class EntitlementChecksService implements EntitlementChecks {
    /**
     * @return the same instance of {@link EntitlementChecksService} returned by {@link EntitlementProvider}.
     */
    public static EntitlementChecksService get() {
        return (EntitlementChecksService) EntitlementProvider.checks();
    }

    private final Set<Module> entitledToExit = newSetFromMap(new ConcurrentHashMap<>());

    public void grant(Module module, Entitlement e) {
        if (isFrozen) {
            throw new IllegalStateException("Entitlement grants are frozen");
        }
        switch (e) {
            case FlagEntitlement f -> {
                switch (f) {
                    case EXIT_JVM -> entitledToExit.add(module);
                }
            }
        }
    }

    /**
     * Mainly for testing purposes.
     */
    public void revokeAll() {
        if (isFrozen) {
            throw new IllegalStateException("Entitlement grants are frozen");
        }
        entitledToExit.clear();
    }

    /**
     * Causes entitlements to be enforced.
     */
    public void activate() {
        isActive = true;
    }

    /**
     * Disallows changes to the entitlement grants.
     */
    public void freeze() {
        isFrozen = true;
    }

    @Override
    public void checkSystemExit(Class<?> callerClass, System system, int status) {
        checkFlagEntitlement(callerClass, entitledToExit);
    }

    private void checkFlagEntitlement(Class<?> callerClass, Set<Module> entitledModules) {
        var requestingModule = requestingModule(callerClass);
        if (isTriviallyAllowed(requestingModule)) {
            // System.out.println(" - Trivially allowed");
            return;
        }
        if (entitledModules.contains(requestingModule)) {
            return;
        }
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
