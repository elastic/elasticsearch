/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.Strings;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PolicyManager {
    private static final Logger logger = LogManager.getLogger(ElasticsearchEntitlementChecker.class);

    protected final Policy serverPolicy;
    protected final Map<String, Policy> pluginPolicies;
    private final Function<Class<?>, String> pluginResolver;

    public static final String ALL_UNNAMED = "ALL-UNNAMED";

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

    public PolicyManager(Policy defaultPolicy, Map<String, Policy> pluginPolicies, Function<Class<?>, String> pluginResolver) {
        this.serverPolicy = Objects.requireNonNull(defaultPolicy);
        this.pluginPolicies = Collections.unmodifiableMap(Objects.requireNonNull(pluginPolicies));
        this.pluginResolver = pluginResolver;
    }

    public void checkFlagEntitlement(Class<?> callerClass, FlagEntitlementType type) {
        var requestingModule = requestingModule(callerClass);
        if (isTriviallyAllowed(requestingModule)) {
            return;
        }

        // TODO: real policy check. For now, we only allow our hardcoded System.exit policy for server.
        // TODO: this will be checked using policies
        if (requestingModule.isNamed()
            && requestingModule.getName().equals("org.elasticsearch.server")
            && (type == FlagEntitlementType.SYSTEM_EXIT || type == FlagEntitlementType.CREATE_CLASSLOADER)) {
            logger.debug("Allowed: caller [{}] in module [{}] has entitlement [{}]", callerClass, requestingModule.getName(), type);
            return;
        }

        // TODO: plugins policy check using pluginResolver and pluginPolicies
        throw new NotEntitledException(
            Strings.format("Missing entitlement [%s] for caller [%s] in module [%s]", type, callerClass, requestingModule.getName())
        );
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

    @Override
    public String toString() {
        return "PolicyManager{" + "serverPolicy=" + serverPolicy + ", pluginPolicies=" + pluginPolicies + '}';
    }
}
