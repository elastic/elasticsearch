/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlements.runtime.api;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.newSetFromMap;
import static org.elasticsearch.entitlements.runtime.api.FlagEntitlement.EXIT_JVM;
import static org.elasticsearch.entitlements.runtime.internals.EntitlementInternals.isActive;

public class EntitlementChecks {
    private static final Set<Module> entitledToExit = newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Causes entitlements to be checked. Before this is called, entitlements are not enforced,
     * so there's no need for an application to set a lot of permissions which are required only during initialization.
     */
    public static void activate() {
        isActive = true;
    }

    public static void revokeAll() {
        entitledToExit.clear();
    }

    public static boolean grant(Module module, Entitlement e) {
        return switch (e) {
            case FlagEntitlement __ -> entitledToExit.add(module);
        };
    }

    private static Module requestingModule(Class<?> callerClass) {
        Module callerModule = callerClass.getModule();
        if (callerModule.getLayer() != ModuleLayer.boot()) {
            // fast path
            return callerModule;
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
        return isActive == false || (requestingModule == null) || requestingModule == EntitlementChecks.class.getModule();
    }

    public static void checkExitJvmEntitlement(Class<?> callerClass) {
        System.out.println("Checking for JVM Exit entitlement on " + callerClass.getSimpleName());
        var requestingModule = requestingModule(callerClass);
        if (isTriviallyAllowed(requestingModule)) {
            System.out.println(" - Trivially allowed");
            return;
        }
        if (entitledToExit.contains(requestingModule)) {
            System.out.println(" - Granted");
            return;
        }
        throw new NotEntitledException("Missing " + EXIT_JVM + " entitlement for " + requestingModule);
    }
}
