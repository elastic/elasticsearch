/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of the {@link EntitlementChecker} interface, providing additional
 * API methods for managing the checks.
 * The trampoline module loads this object via SPI.
 */
public class ElasticsearchEntitlementChecker implements EntitlementChecker {
    private static final Logger logger = LogManager.getLogger(ElasticsearchEntitlementChecker.class);

    private static final Set<Module> systemModules = findSystemModules();

    private static Set<Module> findSystemModules() {
        var systemModulesDescriptors = ModuleFinder.ofSystem()
            .findAll()
            .stream()
            .map(ModuleReference::descriptor)
            .collect(Collectors.toUnmodifiableSet());

        return ModuleLayer.boot()
            .modules()
            .stream()
            .filter(m -> systemModulesDescriptors.contains(m.getDescriptor()))
            .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public void check$java_lang_System$exit(Class<?> callerClass, int status) {
        var requestingModule = requestingModule(callerClass);
        if (isTriviallyAllowed(requestingModule)) {
            return;
        }

        // TODO: this will be checked using policies
        if (requestingModule.isNamed() && requestingModule.getName().equals("org.elasticsearch.server")) {
            logger.debug("Allowed: caller in {} is entitled to exit the JVM", requestingModule.getName());
            return;
        }

        // Hard-forbidden until we develop the permission granting scheme
        throw new NotEntitledException("Missing entitlement for " + requestingModule);
    }

    private static Module requestingModule(Class<?> callerClass) {
        if (callerClass != null) {
            Module callerModule = callerClass.getModule();
            if (systemModules.contains(callerModule) == false) {
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
                    .filter(m -> systemModules.contains(m) == false)
                    .findFirst()
            );
        return module.orElse(null);
    }

    private static boolean isTriviallyAllowed(Module requestingModule) {
        if (requestingModule == null) {
            logger.debug("Trivially allowed: entire call stack is in composed of classes in system modules");
            return true;
        }
        logger.trace("Not trivially allowed");
        return false;
    }
}
