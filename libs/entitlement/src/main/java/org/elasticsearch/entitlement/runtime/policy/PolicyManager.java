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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PolicyManager {
    private static final Logger logger = LogManager.getLogger(ElasticsearchEntitlementChecker.class);

    static class ModuleEntitlements {
        public static final ModuleEntitlements NONE = new ModuleEntitlements(List.of());
        private final IdentityHashMap<Class<? extends Entitlement>, List<Entitlement>> entitlementsByType;

        ModuleEntitlements(List<Entitlement> entitlements) {
            this.entitlementsByType = entitlements.stream()
                .collect(Collectors.toMap(Entitlement::getClass, e -> new ArrayList<>(List.of(e)), (a, b) -> {
                    a.addAll(b);
                    return a;
                }, IdentityHashMap::new));
        }

        public boolean hasEntitlement(Class<? extends Entitlement> entitlementClass) {
            return entitlementsByType.containsKey(entitlementClass);
        }

        public <E extends Entitlement> Stream<E> getEntitlements(Class<E> entitlementClass) {
            return entitlementsByType.get(entitlementClass).stream().map(entitlementClass::cast);
        }
    }

    final Map<Module, ModuleEntitlements> moduleEntitlementsMap = new HashMap<>();

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

    private static List<Entitlement> lookupEntitlementsForModule(Policy policy, String moduleName) {
        for (int i = 0; i < policy.scopes.size(); ++i) {
            var scope = policy.scopes.get(i);
            if (scope.name.equals(moduleName)) {
                return scope.entitlements;
            }
        }
        return null;
    }

    public void checkExitVM(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, ExitVMEntitlement.class);
    }

    public void checkCreateClassLoader(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, CreateClassLoaderEntitlement.class);
    }

    private void checkEntitlementPresent(Class<?> callerClass, Class<? extends Entitlement> entitlementClass) {
        var requestingModule = requestingModule(callerClass);
        if (isTriviallyAllowed(requestingModule)) {
            return;
        }

        ModuleEntitlements entitlements = getEntitlementsOrThrow(callerClass, requestingModule);
        if (entitlements.hasEntitlement(entitlementClass)) {
            logger.debug(
                () -> Strings.format(
                    "Entitled: caller [%s], module [%s], type [%s]",
                    callerClass,
                    requestingModule.getName(),
                    entitlementClass.getSimpleName()
                )
            );
            return;
        }
        throw new NotEntitledException(
            Strings.format(
                "Missing entitlement: caller [%s], module [%s], type [%s]",
                callerClass,
                requestingModule.getName(),
                entitlementClass.getSimpleName()
            )
        );
    }

    ModuleEntitlements getEntitlementsOrThrow(Class<?> callerClass, Module requestingModule) {
        ModuleEntitlements cachedEntitlement = moduleEntitlementsMap.get(requestingModule);
        if (cachedEntitlement != null) {
            if (cachedEntitlement == ModuleEntitlements.NONE) {
                throw new NotEntitledException(buildModuleNoPolicyMessage(callerClass, requestingModule) + "[CACHED]");
            }
            return cachedEntitlement;
        }

        if (isServerModule(requestingModule)) {
            var scopeName = requestingModule.getName();
            return getModuleEntitlementsOrThrow(callerClass, requestingModule, serverPolicy, scopeName);
        }

        // plugins
        var pluginName = pluginResolver.apply(callerClass);
        if (pluginName != null) {
            var pluginPolicy = pluginPolicies.get(pluginName);
            if (pluginPolicy != null) {
                final String scopeName;
                if (requestingModule.isNamed() == false) {
                    scopeName = ALL_UNNAMED;
                } else {
                    scopeName = requestingModule.getName();
                }
                return getModuleEntitlementsOrThrow(callerClass, requestingModule, pluginPolicy, scopeName);
            }
        }

        moduleEntitlementsMap.put(requestingModule, ModuleEntitlements.NONE);
        throw new NotEntitledException(buildModuleNoPolicyMessage(callerClass, requestingModule));
    }

    private static String buildModuleNoPolicyMessage(Class<?> callerClass, Module requestingModule) {
        return Strings.format("Missing entitlement policy: caller [%s], module [%s]", callerClass, requestingModule.getName());
    }

    private ModuleEntitlements getModuleEntitlementsOrThrow(Class<?> callerClass, Module module, Policy policy, String moduleName) {
        var entitlements = lookupEntitlementsForModule(policy, moduleName);
        if (entitlements == null) {
            // Module without entitlements - remember we don't have any
            moduleEntitlementsMap.put(module, ModuleEntitlements.NONE);
            throw new NotEntitledException(buildModuleNoPolicyMessage(callerClass, module));
        }
        // We have a policy for this module
        var classEntitlements = createClassEntitlements(entitlements);
        moduleEntitlementsMap.put(module, classEntitlements);
        return classEntitlements;
    }

    private static boolean isServerModule(Module requestingModule) {
        return requestingModule.isNamed() && requestingModule.getLayer() == ModuleLayer.boot();
    }

    private ModuleEntitlements createClassEntitlements(List<Entitlement> entitlements) {
        return new ModuleEntitlements(entitlements);
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
            logger.debug("Entitlement trivially allowed: entire call stack is in composed of classes in system modules");
            return true;
        }
        logger.trace("Entitlement not trivially allowed");
        return false;
    }

    @Override
    public String toString() {
        return "PolicyManager{" + "serverPolicy=" + serverPolicy + ", pluginPolicies=" + pluginPolicies + '}';
    }
}
