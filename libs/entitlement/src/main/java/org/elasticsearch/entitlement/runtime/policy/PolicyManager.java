/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PolicyManager {
    private static final Logger logger = LogManager.getLogger(ElasticsearchEntitlementChecker.class);

    protected final Map<String, List<Entitlement>> mainPolicies;
    protected final Map<String, Policy> pluginPolicies;
    private final Function<Class<?>, String> pluginResolver;

    public static final String ALL_UNNAMED = "ALL-UNNAMED";

    public PolicyManager(Policy defaultPolicy, Map<String, Policy> pluginPolicies, Function<Class<?>, String> pluginResolver) {
        this.mainPolicies = Objects.requireNonNull(defaultPolicy).scopes.stream()
            .collect(Collectors.toUnmodifiableMap(e -> e.name, e -> e.entitlements));
        this.pluginPolicies = Collections.unmodifiableMap(Objects.requireNonNull(pluginPolicies));
        this.pluginResolver = pluginResolver;
    }

    public void checkFlagEntitlement(Class<?> callerClass, FlagEntitlementType type) {
        var requestingModule = requestingModule(callerClass);
        if (isTriviallyAllowed(requestingModule)) {
            return;
        }

        // TODO: real policy check. For now, we only allow our hardcoded System.exit policy for server.
        if (requestingModule == System.class.getModule() && type == FlagEntitlementType.SYSTEM_EXIT) {
            logger.debug("Allowed: caller in module {} has entitlement SYSTEM_EXIT", System.class.getModule().getName());
            return;
        }

        // TODO: plugins policy check using pluginResolver and pluginPolicies

        // Hard-forbidden until we develop the permission granting scheme
        throw new NotEntitledException("Missing entitlement for " + requestingModule);
    }

    // TODO: FIXME (this does not work, as all elastic modules end up in the boot layer)
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
        if (requestingModule == null) {
            logger.debug("Trivially allowed: Entire call stack is in the boot module layer");
            return true;
        }
        logger.trace("Not trivially allowed");
        return false;
    }

    @Override
    public String toString() {
        return "PolicyManager{" + "mainPolicies=" + mainPolicies + ", pluginPolicies=" + pluginPolicies + '}';
    }
}
