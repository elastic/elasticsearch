/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.policy.FileAccessTree.ExclusiveFileEntitlement;
import org.elasticsearch.entitlement.runtime.policy.FileAccessTree.ExclusivePath;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.elasticsearch.entitlement.bridge.Util.NO_CLASS;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.APM_AGENT;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.PLUGIN;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.SERVER;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.UNKNOWN;

/**
 * Determines, from the specified policy information, which entitlements are granted to a given caller class,
 * as well as whether certain caller classes (like those built into the JDK) should be <em>trivially allowed</em>,
 * meaning they are always entitled regardless of policy.
 */
public class PolicyManager {
    public static final String ALL_UNNAMED = "ALL-UNNAMED";
    /**
     * Use this if you don't have a {@link ModuleEntitlements} in hand.
     */
    static final Logger generalLogger = LogManager.getLogger(PolicyManager.class);

    static final Set<String> MODULES_EXCLUDED_FROM_SYSTEM_MODULES = Set.of("java.desktop");

    /**
     * Identifies a particular entitlement {@link Scope} within a {@link Policy}.
     */
    public record PolicyScope(ComponentKind kind, String componentName, String moduleName) {
        public PolicyScope {
            requireNonNull(kind);
            requireNonNull(componentName);
            requireNonNull(moduleName);
            assert kind.componentName == null || kind.componentName.equals(componentName);
        }

        public static PolicyScope unknown(String moduleName) {
            return new PolicyScope(UNKNOWN, UNKNOWN.componentName, moduleName);
        }

        public static PolicyScope server(String moduleName) {
            return new PolicyScope(SERVER, SERVER.componentName, moduleName);
        }

        public static PolicyScope apmAgent(String moduleName) {
            return new PolicyScope(APM_AGENT, APM_AGENT.componentName, moduleName);
        }

        public static PolicyScope plugin(String componentName, String moduleName) {
            return new PolicyScope(PLUGIN, componentName, moduleName);
        }
    }

    public enum ComponentKind {
        UNKNOWN("(unknown)"),
        SERVER("(server)"),
        APM_AGENT("(APM agent)"),
        PLUGIN(null);

        /**
         * If this kind corresponds to a single component, this is that component's name;
         * otherwise null.
         */
        final String componentName;

        ComponentKind(String componentName) {
            this.componentName = componentName;
        }
    }

    /**
     * This class contains all the entitlements by type, plus the {@link FileAccessTree} for the special case of filesystem entitlements.
     * <p>
     * We use layers when computing {@link ModuleEntitlements}; first, we check whether the module we are building it for is in the
     * server layer ({@link PolicyManager#SERVER_LAYER_MODULES}) (*).
     * If it is, we use the server policy, using the same caller class module name as the scope, and read the entitlements for that scope.
     * Otherwise, we use the {@code PluginResolver} to identify the correct plugin layer and find the policy for it (if any).
     * If the plugin is modular, we again use the same caller class module name as the scope, and read the entitlements for that scope.
     * If it's not, we use the single {@code ALL-UNNAMED} scope – in this case there is one scope and all entitlements apply
     * to all the plugin code.
     * </p>
     * <p>
     * (*) implementation detail: this is currently done in an indirect way: we know the module is not in the system layer
     * (otherwise the check would have been already trivially allowed), so we just check that the module is named, and it belongs to the
     * boot {@link ModuleLayer}. We might want to change this in the future to make it more consistent/easier to maintain.
     * </p>
     *
     * @param componentName the plugin name or else one of the special component names like "(server)".
     */
    protected record ModuleEntitlements(
        String componentName,
        Map<Class<? extends Entitlement>, List<Entitlement>> entitlementsByType,
        FileAccessTree fileAccess,
        Logger logger
    ) {

        public ModuleEntitlements {
            entitlementsByType = Map.copyOf(entitlementsByType);
        }

        public boolean hasEntitlement(Class<? extends Entitlement> entitlementClass) {
            return entitlementsByType.containsKey(entitlementClass);
        }

        public <E extends Entitlement> Stream<E> getEntitlements(Class<E> entitlementClass) {
            var entitlements = entitlementsByType.get(entitlementClass);
            if (entitlements == null) {
                return Stream.empty();
            }
            return entitlements.stream().map(entitlementClass::cast);
        }
    }

    private FileAccessTree getDefaultFileAccess(Collection<Path> componentPaths) {
        return FileAccessTree.withoutExclusivePaths(FilesEntitlement.EMPTY, pathLookup, componentPaths);
    }

    // pkg private for testing
    ModuleEntitlements defaultEntitlements(String componentName, Collection<Path> componentPaths, String moduleName) {
        return new ModuleEntitlements(componentName, Map.of(), getDefaultFileAccess(componentPaths), getLogger(componentName, moduleName));
    }

    // pkg private for testing
    ModuleEntitlements policyEntitlements(
        String componentName,
        Collection<Path> componentPaths,
        String moduleName,
        List<Entitlement> entitlements
    ) {
        FilesEntitlement filesEntitlement = FilesEntitlement.EMPTY;
        for (Entitlement entitlement : entitlements) {
            if (entitlement instanceof FilesEntitlement) {
                filesEntitlement = (FilesEntitlement) entitlement;
            }
        }
        return new ModuleEntitlements(
            componentName,
            entitlements.stream().collect(groupingBy(Entitlement::getClass)),
            FileAccessTree.of(componentName, moduleName, filesEntitlement, pathLookup, componentPaths, exclusivePaths),
            getLogger(componentName, moduleName)
        );
    }

    final Map<Module, ModuleEntitlements> moduleEntitlementsMap = new ConcurrentHashMap<>();

    private final Map<String, List<Entitlement>> serverEntitlements;
    private final List<Entitlement> apmAgentEntitlements;
    private final Map<String, Map<String, List<Entitlement>>> pluginsEntitlements;
    private final Function<Class<?>, PolicyScope> scopeResolver;
    private final PathLookup pathLookup;

    private static final Set<Module> SYSTEM_LAYER_MODULES = findSystemLayerModules();

    private static Set<Module> findSystemLayerModules() {
        var systemModulesDescriptors = ModuleFinder.ofSystem()
            .findAll()
            .stream()
            .map(ModuleReference::descriptor)
            .collect(Collectors.toUnmodifiableSet());
        return Stream.concat(
            // entitlements is a "system" module, we can do anything from it
            Stream.of(PolicyManager.class.getModule()),
            // anything in the boot layer is also part of the system
            ModuleLayer.boot()
                .modules()
                .stream()
                .filter(
                    m -> systemModulesDescriptors.contains(m.getDescriptor())
                        && MODULES_EXCLUDED_FROM_SYSTEM_MODULES.contains(m.getName()) == false
                )
        ).collect(Collectors.toUnmodifiableSet());
    }

    // Anything in the boot layer that is not in the system layer, is in the server layer
    public static final Set<Module> SERVER_LAYER_MODULES = ModuleLayer.boot()
        .modules()
        .stream()
        .filter(m -> SYSTEM_LAYER_MODULES.contains(m) == false)
        .collect(Collectors.toUnmodifiableSet());

    private final Map<String, Collection<Path>> pluginSourcePaths;

    /**
     * Paths that are only allowed for a single module. Used to generate
     * structures to indicate other modules aren't allowed to use these
     * files in {@link FileAccessTree}s.
     */
    private final List<ExclusivePath> exclusivePaths;

    public PolicyManager(
        Policy serverPolicy,
        List<Entitlement> apmAgentEntitlements,
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, PolicyScope> scopeResolver,
        Map<String, Collection<Path>> pluginSourcePaths,
        PathLookup pathLookup
    ) {
        this.serverEntitlements = buildScopeEntitlementsMap(requireNonNull(serverPolicy));
        this.apmAgentEntitlements = apmAgentEntitlements;
        this.pluginsEntitlements = requireNonNull(pluginPolicies).entrySet()
            .stream()
            .collect(toUnmodifiableMap(Map.Entry::getKey, e -> buildScopeEntitlementsMap(e.getValue())));
        this.scopeResolver = scopeResolver;
        this.pluginSourcePaths = pluginSourcePaths;
        this.pathLookup = requireNonNull(pathLookup);

        List<ExclusiveFileEntitlement> exclusiveFileEntitlements = new ArrayList<>();
        for (var e : serverEntitlements.entrySet()) {
            validateEntitlementsPerModule(SERVER.componentName, e.getKey(), e.getValue(), exclusiveFileEntitlements);
        }
        validateEntitlementsPerModule(APM_AGENT.componentName, ALL_UNNAMED, apmAgentEntitlements, exclusiveFileEntitlements);
        for (var p : pluginsEntitlements.entrySet()) {
            for (var m : p.getValue().entrySet()) {
                validateEntitlementsPerModule(p.getKey(), m.getKey(), m.getValue(), exclusiveFileEntitlements);
            }
        }
        List<ExclusivePath> exclusivePaths = FileAccessTree.buildExclusivePathList(
            exclusiveFileEntitlements,
            pathLookup,
            FileAccessTree.DEFAULT_COMPARISON
        );
        FileAccessTree.validateExclusivePaths(exclusivePaths, FileAccessTree.DEFAULT_COMPARISON);
        this.exclusivePaths = exclusivePaths;
    }

    private static Map<String, List<Entitlement>> buildScopeEntitlementsMap(Policy policy) {
        return policy.scopes().stream().collect(toUnmodifiableMap(Scope::moduleName, Scope::entitlements));
    }

    private static void validateEntitlementsPerModule(
        String componentName,
        String moduleName,
        List<Entitlement> entitlements,
        List<ExclusiveFileEntitlement> exclusiveFileEntitlements
    ) {
        Set<Class<? extends Entitlement>> found = new HashSet<>();
        for (var e : entitlements) {
            if (found.contains(e.getClass())) {
                throw new IllegalArgumentException(
                    "[" + componentName + "] using module [" + moduleName + "] found duplicate entitlement [" + e.getClass().getName() + "]"
                );
            }
            found.add(e.getClass());
            if (e instanceof FilesEntitlement fe) {
                exclusiveFileEntitlements.add(new ExclusiveFileEntitlement(componentName, moduleName, fe));
            }
        }
    }

    private static Logger getLogger(String componentName, String moduleName) {
        var loggerSuffix = "." + componentName + "." + ((moduleName == null) ? ALL_UNNAMED : moduleName);
        return MODULE_LOGGERS.computeIfAbsent(PolicyManager.class.getName() + loggerSuffix, LogManager::getLogger);
    }

    /**
     * We want to use the same {@link Logger} object for a given name, because we want {@link ModuleEntitlements}
     * {@code equals} and {@code hashCode} to work.
     * <p>
     * This would not be required if LogManager
     * <a href="https://github.com/elastic/elasticsearch/issues/87511">memoized the loggers</a>,
     * but here we are.
     */
    private static final ConcurrentHashMap<String, Logger> MODULE_LOGGERS = new ConcurrentHashMap<>();

    protected ModuleEntitlements getEntitlements(Class<?> requestingClass) {
        if ("io.netty.channel.socket.nio.NioSocketChannel".equals(requestingClass.getName())) {
            System.err.println("PATDOYLE here we go");
        }
        return moduleEntitlementsMap.computeIfAbsent(requestingClass.getModule(), m -> computeEntitlements(requestingClass));
    }

    protected final ModuleEntitlements computeEntitlements(Class<?> requestingClass) {
        var policyScope = scopeResolver.apply(requestingClass);
        var componentName = policyScope.componentName();
        var moduleName = policyScope.moduleName();

        switch (policyScope.kind()) {
            case SERVER -> {
                return getModuleScopeEntitlements(
                    serverEntitlements,
                    moduleName,
                    SERVER.componentName,
                    List.of(getComponentPathFromClass(requestingClass))
                );
            }
            case APM_AGENT -> {
                // The APM agent is the only thing running non-modular in the system classloader
                return policyEntitlements(
                    APM_AGENT.componentName,
                    List.of(getComponentPathFromClass(requestingClass)),
                    ALL_UNNAMED,
                    apmAgentEntitlements
                );
            }
            case UNKNOWN -> {
                return defaultEntitlements(UNKNOWN.componentName, null, moduleName);
            }
            default -> {
                assert policyScope.kind() == PLUGIN;
                var pluginEntitlements = pluginsEntitlements.get(componentName);
                if (pluginEntitlements == null) {
                    return defaultEntitlements(componentName, pluginSourcePaths.get(componentName), moduleName);
                } else {
                    return getModuleScopeEntitlements(pluginEntitlements, moduleName, componentName, pluginSourcePaths.get(componentName));
                }
            }
        }
    }

    protected Path getComponentPathFromClass(Class<?> requestingClass) {
        var codeSource = requestingClass.getProtectionDomain().getCodeSource();
        if (codeSource == null) {
            return null;
        }
        try {
            return Paths.get(codeSource.getLocation().toURI());
        } catch (Exception e) {
            // If we get a URISyntaxException, or any other Exception due to an invalid URI, we return null to safely skip this location
            generalLogger.info(
                "Cannot get component path for [{}]: [{}] cannot be converted to a valid Path",
                requestingClass.getName(),
                codeSource.getLocation().toString()
            );
            return null;
        }
    }

    private ModuleEntitlements getModuleScopeEntitlements(
        Map<String, List<Entitlement>> scopeEntitlements,
        String scopeName,
        String componentName,
        Collection<Path> componentPaths
    ) {
        var entitlements = scopeEntitlements.get(scopeName);
        if (entitlements == null) {
            return defaultEntitlements(componentName, componentPaths, scopeName);
        }
        return policyEntitlements(componentName, componentPaths, scopeName, entitlements);
    }

    /**
     * @return true if permission is granted regardless of the entitlement
     */
    boolean isTriviallyAllowed(Class<?> requestingClass) {
        if (generalLogger.isTraceEnabled()) {
            generalLogger.trace("Stack trace for upcoming trivially-allowed check", new Exception());
        }
        if (requestingClass == null) {
            generalLogger.debug("Entitlement trivially allowed: no caller frames outside the entitlement library");
            return true;
        }
        if (requestingClass == NO_CLASS) {
            generalLogger.debug("Entitlement trivially allowed from outermost frame");
            return true;
        }
        if (isTrustedSystemClass(requestingClass)) {
            generalLogger.debug("Entitlement trivially allowed from system module [{}]", requestingClass.getModule().getName());
            return true;
        }
        generalLogger.trace("Entitlement not trivially allowed");
        return false;
    }

    /**
     * The main decision point for what counts as a trusted built-in JDK class.
     */
    protected boolean isTrustedSystemClass(Class<?> requestingClass) {
        return SYSTEM_LAYER_MODULES.contains(requestingClass.getModule());
    }

    @Override
    public String toString() {
        return "PolicyManager{" + "serverEntitlements=" + serverEntitlements + ", pluginsEntitlements=" + pluginsEntitlements + '}';
    }
}
