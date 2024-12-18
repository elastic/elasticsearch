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

import java.lang.StackWalker.StackFrame;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.util.ArrayList;
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

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

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

    protected final Map<String, List<Entitlement>> serverEntitlements;
    protected final Map<String, Map<String, List<Entitlement>>> pluginsEntitlements;
    private final Function<Class<?>, String> pluginResolver;

    public static final String ALL_UNNAMED = "ALL-UNNAMED";

    private static final Set<Module> systemModules = findSystemModules();

    /**
     * Frames originating from this module are ignored in the permission logic.
     */
    private final Module entitlementsModule;

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

    public PolicyManager(
        Policy defaultPolicy,
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, String> pluginResolver,
        Module entitlementsModule
    ) {
        this.serverEntitlements = buildScopeEntitlementsMap(requireNonNull(defaultPolicy));
        this.pluginsEntitlements = requireNonNull(pluginPolicies).entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> buildScopeEntitlementsMap(e.getValue())));
        this.pluginResolver = pluginResolver;
        this.entitlementsModule = entitlementsModule;
    }

    private static Map<String, List<Entitlement>> buildScopeEntitlementsMap(Policy policy) {
        return policy.scopes.stream().collect(Collectors.toUnmodifiableMap(scope -> scope.name, scope -> scope.entitlements));
    }

    public void checkStartProcess(Class<?> callerClass) {
        neverEntitled(callerClass, "start process");
    }

    private void neverEntitled(Class<?> callerClass, String operationDescription) {
        var requestingModule = requestingModule(callerClass);
        if (isTriviallyAllowed(requestingModule)) {
            return;
        }

        throw new NotEntitledException(
            Strings.format(
                "Not entitled: caller [%s], module [%s], operation [%s]",
                callerClass,
                requestingModule.getName(),
                operationDescription
            )
        );
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
            return getModuleEntitlementsOrThrow(callerClass, requestingModule, serverEntitlements, scopeName);
        }

        // plugins
        var pluginName = pluginResolver.apply(callerClass);
        if (pluginName != null) {
            var pluginEntitlements = pluginsEntitlements.get(pluginName);
            if (pluginEntitlements != null) {
                final String scopeName;
                if (requestingModule.isNamed() == false) {
                    scopeName = ALL_UNNAMED;
                } else {
                    scopeName = requestingModule.getName();
                }
                return getModuleEntitlementsOrThrow(callerClass, requestingModule, pluginEntitlements, scopeName);
            }
        }

        moduleEntitlementsMap.put(requestingModule, ModuleEntitlements.NONE);
        throw new NotEntitledException(buildModuleNoPolicyMessage(callerClass, requestingModule));
    }

    private static String buildModuleNoPolicyMessage(Class<?> callerClass, Module requestingModule) {
        return Strings.format("Missing entitlement policy: caller [%s], module [%s]", callerClass, requestingModule.getName());
    }

    private ModuleEntitlements getModuleEntitlementsOrThrow(
        Class<?> callerClass,
        Module module,
        Map<String, List<Entitlement>> scopeEntitlements,
        String moduleName
    ) {
        var entitlements = scopeEntitlements.get(moduleName);
        if (entitlements == null) {
            // Module without entitlements - remember we don't have any
            moduleEntitlementsMap.put(module, ModuleEntitlements.NONE);
            throw new NotEntitledException(buildModuleNoPolicyMessage(callerClass, module));
        }
        // We have a policy for this module
        var classEntitlements = new ModuleEntitlements(entitlements);
        moduleEntitlementsMap.put(module, classEntitlements);
        return classEntitlements;
    }

    private static boolean isServerModule(Module requestingModule) {
        return requestingModule.isNamed() && requestingModule.getLayer() == ModuleLayer.boot();
    }

    /**
     * Walks the stack to determine which module's entitlements should be checked.
     *
     * @param callerClass when non-null will be used if its module is suitable;
     *                    this is a fast-path check that can avoid the stack walk
     *                    in cases where the caller class is available.
     * @return the requesting module, or {@code null} if the entire call stack
     * comes from modules that are trusted.
     */
    Module requestingModule(Class<?> callerClass) {
        if (callerClass != null) {
            Module callerModule = callerClass.getModule();
            if (systemModules.contains(callerModule) == false) {
                // fast path
                return callerModule;
            }
        }
        Optional<Module> module = StackWalker.getInstance(RETAIN_CLASS_REFERENCE)
            .walk(frames -> findRequestingModule(frames.map(StackFrame::getDeclaringClass)));
        return module.orElse(null);
    }

    /**
     * Given a stream of classes corresponding to the frames from a {@link StackWalker},
     * returns the module whose entitlements should be checked.
     *
     * @throws NullPointerException if the requesting module is {@code null}
     */
    Optional<Module> findRequestingModule(Stream<Class<?>> classes) {
        return classes.map(Objects::requireNonNull)
            .map(PolicyManager::moduleOf)
            .filter(m -> m != entitlementsModule)  // Ignore the entitlements library itself
            .filter(not(systemModules::contains))  // Skip trusted JDK modules
            .findFirst();
    }

    private static Module moduleOf(Class<?> c) {
        var result = c.getModule();
        if (result == null) {
            throw new NullPointerException("Entitlements system does not support non-modular class [" + c.getName() + "]");
        } else {
            return result;
        }
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
        return "PolicyManager{" + "serverEntitlements=" + serverEntitlements + ", pluginsEntitlements=" + pluginsEntitlements + '}';
    }
}
