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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ManageThreadsEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ReadStoreAttributesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.SetHttpsConnectionPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.File;
import java.lang.StackWalker.StackFrame;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class PolicyManager {
    private static final Logger logger = LogManager.getLogger(PolicyManager.class);

    static final String UNKNOWN_COMPONENT_NAME = "(unknown)";
    static final String SERVER_COMPONENT_NAME = "(server)";
    static final String APM_AGENT_COMPONENT_NAME = "(APM agent)";

    /**
     * @param componentName the plugin name; or else one of the special component names
     *                      like {@link #SERVER_COMPONENT_NAME} or {@link #APM_AGENT_COMPONENT_NAME}.
     */
    record ModuleEntitlements(
        String componentName,
        Map<Class<? extends Entitlement>, List<Entitlement>> entitlementsByType,
        FileAccessTree fileAccess
    ) {

        ModuleEntitlements {
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

    // pkg private for testing
    ModuleEntitlements defaultEntitlements(String componentName) {
        return new ModuleEntitlements(componentName, Map.of(), defaultFileAccess);
    }

    // pkg private for testing
    ModuleEntitlements policyEntitlements(String componentName, List<Entitlement> entitlements) {
        FilesEntitlement filesEntitlement = FilesEntitlement.EMPTY;
        for (Entitlement entitlement : entitlements) {
            if (entitlement instanceof FilesEntitlement) {
                filesEntitlement = (FilesEntitlement) entitlement;
            }
        }
        return new ModuleEntitlements(
            componentName,
            entitlements.stream().collect(groupingBy(Entitlement::getClass)),
            FileAccessTree.of(filesEntitlement, pathLookup)
        );
    }

    final Map<Module, ModuleEntitlements> moduleEntitlementsMap = new ConcurrentHashMap<>();

    private final Map<String, List<Entitlement>> serverEntitlements;
    private final List<Entitlement> apmAgentEntitlements;
    private final Map<String, Map<String, List<Entitlement>>> pluginsEntitlements;
    private final Function<Class<?>, String> pluginResolver;
    private final PathLookup pathLookup;
    private final FileAccessTree defaultFileAccess;

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

    /**
     * The package name containing classes from the APM agent.
     */
    private final String apmAgentPackageName;

    /**
     * Frames originating from this module are ignored in the permission logic.
     */
    private final Module entitlementsModule;

    public PolicyManager(
        Policy serverPolicy,
        List<Entitlement> apmAgentEntitlements,
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, String> pluginResolver,
        String apmAgentPackageName,
        Module entitlementsModule,
        PathLookup pathLookup
    ) {
        this.serverEntitlements = buildScopeEntitlementsMap(requireNonNull(serverPolicy));
        this.apmAgentEntitlements = apmAgentEntitlements;
        this.pluginsEntitlements = requireNonNull(pluginPolicies).entrySet()
            .stream()
            .collect(toUnmodifiableMap(Map.Entry::getKey, e -> buildScopeEntitlementsMap(e.getValue())));
        this.pluginResolver = pluginResolver;
        this.apmAgentPackageName = apmAgentPackageName;
        this.entitlementsModule = entitlementsModule;
        this.pathLookup = requireNonNull(pathLookup);
        this.defaultFileAccess = FileAccessTree.of(FilesEntitlement.EMPTY, pathLookup);

        for (var e : serverEntitlements.entrySet()) {
            validateEntitlementsPerModule(SERVER_COMPONENT_NAME, e.getKey(), e.getValue());
        }
        validateEntitlementsPerModule(APM_AGENT_COMPONENT_NAME, "unnamed", apmAgentEntitlements);
        for (var p : pluginsEntitlements.entrySet()) {
            for (var m : p.getValue().entrySet()) {
                validateEntitlementsPerModule(p.getKey(), m.getKey(), m.getValue());
            }
        }
    }

    private static Map<String, List<Entitlement>> buildScopeEntitlementsMap(Policy policy) {
        return policy.scopes().stream().collect(toUnmodifiableMap(Scope::moduleName, Scope::entitlements));
    }

    private static void validateEntitlementsPerModule(String componentName, String moduleName, List<Entitlement> entitlements) {
        Set<Class<? extends Entitlement>> found = new HashSet<>();
        for (var e : entitlements) {
            if (found.contains(e.getClass())) {
                throw new IllegalArgumentException(
                    "[" + componentName + "] using module [" + moduleName + "] found duplicate entitlement [" + e.getClass().getName() + "]"
                );
            }
            found.add(e.getClass());
        }
    }

    public void checkStartProcess(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "start process");
    }

    public void checkWriteStoreAttributes(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "change file store attributes");
    }

    public void checkReadStoreAttributes(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, ReadStoreAttributesEntitlement.class);
    }

    /**
     * @param operationDescription is only called when the operation is not trivially allowed, meaning the check is about to fail;
     *                            therefore, its performance is not a major concern.
     */
    private void neverEntitled(Class<?> callerClass, Supplier<String> operationDescription) {
        var requestingClass = requestingClass(callerClass);
        if (isTriviallyAllowed(requestingClass)) {
            return;
        }

        notEntitled(
            Strings.format(
                "Not entitled: component [%s], module [%s], class [%s], operation [%s]",
                getEntitlements(requestingClass).componentName(),
                requestingClass.getModule().getName(),
                requestingClass,
                operationDescription.get()
            )
        );
    }

    public void checkExitVM(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, ExitVMEntitlement.class);
    }

    public void checkCreateClassLoader(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, CreateClassLoaderEntitlement.class);
    }

    public void checkSetHttpsConnectionProperties(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, SetHttpsConnectionPropertiesEntitlement.class);
    }

    public void checkChangeJVMGlobalState(Class<?> callerClass) {
        neverEntitled(callerClass, () -> walkStackForCheckMethodName().orElse("change JVM global state"));
    }

    private Optional<String> walkStackForCheckMethodName() {
        // Look up the check$ method to compose an informative error message.
        // This way, we don't need to painstakingly describe every individual global-state change.
        return StackWalker.getInstance()
            .walk(
                frames -> frames.map(StackFrame::getMethodName)
                    .dropWhile(not(methodName -> methodName.startsWith(InstrumentationService.CHECK_METHOD_PREFIX)))
                    .findFirst()
            )
            .map(this::operationDescription);
    }

    /**
     * Check for operations that can modify the way network operations are handled
     */
    public void checkChangeNetworkHandling(Class<?> callerClass) {
        checkChangeJVMGlobalState(callerClass);
    }

    @SuppressForbidden(reason = "Explicitly checking File apis")
    public void checkFileRead(Class<?> callerClass, File file) {
        checkFileRead(callerClass, file.toPath());
    }

    public void checkFileRead(Class<?> callerClass, Path path) {
        var requestingClass = requestingClass(callerClass);
        if (isTriviallyAllowed(requestingClass)) {
            return;
        }

        ModuleEntitlements entitlements = getEntitlements(requestingClass);
        if (entitlements.fileAccess().canRead(path) == false) {
            notEntitled(
                Strings.format(
                    "Not entitled: component [%s], module [%s], class [%s], entitlement [file], operation [read], path [%s]",
                    entitlements.componentName(),
                    requestingClass.getModule().getName(),
                    requestingClass,
                    path
                )
            );
        }
    }

    @SuppressForbidden(reason = "Explicitly checking File apis")
    public void checkFileWrite(Class<?> callerClass, File file) {
        checkFileWrite(callerClass, file.toPath());
    }

    public void checkFileWrite(Class<?> callerClass, Path path) {
        var requestingClass = requestingClass(callerClass);
        if (isTriviallyAllowed(requestingClass)) {
            return;
        }

        ModuleEntitlements entitlements = getEntitlements(requestingClass);
        if (entitlements.fileAccess().canWrite(path) == false) {
            notEntitled(
                Strings.format(
                    "Not entitled: component [%s], module [%s], class [%s], entitlement [file], operation [write], path [%s]",
                    entitlements.componentName(),
                    requestingClass.getModule().getName(),
                    requestingClass,
                    path
                )
            );
        }
    }

    /**
     * Invoked when we try to get an arbitrary {@code FileAttributeView} class. Such a class can modify attributes, like owner etc.;
     * we could think about introducing checks for each of the operations, but for now we over-approximate this and simply deny when it is
     * used directly.
     */
    public void checkGetFileAttributeView(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "get file attribute view");
    }

    /**
     * Check for operations that can access sensitive network information, e.g. secrets, tokens or SSL sessions
     */
    public void checkLoadingNativeLibraries(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, LoadNativeLibrariesEntitlement.class);
    }

    private String operationDescription(String methodName) {
        // TODO: Use a more human-readable description. Perhaps share code with InstrumentationServiceImpl.parseCheckerMethodName
        return methodName.substring(methodName.indexOf('$'));
    }

    public void checkInboundNetworkAccess(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, InboundNetworkEntitlement.class);
    }

    public void checkOutboundNetworkAccess(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, OutboundNetworkEntitlement.class);
    }

    public void checkAllNetworkAccess(Class<?> callerClass) {
        var requestingClass = requestingClass(callerClass);
        if (isTriviallyAllowed(requestingClass)) {
            return;
        }

        var classEntitlements = getEntitlements(requestingClass);
        checkFlagEntitlement(classEntitlements, InboundNetworkEntitlement.class, requestingClass);
        checkFlagEntitlement(classEntitlements, OutboundNetworkEntitlement.class, requestingClass);
    }

    private static void checkFlagEntitlement(
        ModuleEntitlements classEntitlements,
        Class<? extends Entitlement> entitlementClass,
        Class<?> requestingClass
    ) {
        if (classEntitlements.hasEntitlement(entitlementClass) == false) {
            notEntitled(
                Strings.format(
                    "Not entitled: component [%s], module [%s], class [%s], entitlement [%s]",
                    classEntitlements.componentName(),
                    requestingClass.getModule().getName(),
                    requestingClass,
                    PolicyParser.getEntitlementTypeName(entitlementClass)
                )
            );
        }
        logger.debug(
            () -> Strings.format(
                "Entitled: component [%s], module [%s], class [%s], entitlement [%s]",
                classEntitlements.componentName(),
                requestingClass.getModule().getName(),
                requestingClass,
                PolicyParser.getEntitlementTypeName(entitlementClass)
            )
        );
    }

    public void checkWriteProperty(Class<?> callerClass, String property) {
        var requestingClass = requestingClass(callerClass);
        if (isTriviallyAllowed(requestingClass)) {
            return;
        }

        ModuleEntitlements entitlements = getEntitlements(requestingClass);
        if (entitlements.getEntitlements(WriteSystemPropertiesEntitlement.class).anyMatch(e -> e.properties().contains(property))) {
            logger.debug(
                () -> Strings.format(
                    "Entitled: component [%s], module [%s], class [%s], entitlement [write_system_properties], property [%s]",
                    entitlements.componentName(),
                    requestingClass.getModule().getName(),
                    requestingClass,
                    property
                )
            );
            return;
        }
        notEntitled(
            Strings.format(
                "Not entitled: component [%s], module [%s], class [%s], entitlement [write_system_properties], property [%s]",
                entitlements.componentName(),
                requestingClass.getModule().getName(),
                requestingClass,
                property
            )
        );
    }

    private static void notEntitled(String message) {
        throw new NotEntitledException(message);
    }

    public void checkManageThreadsEntitlement(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, ManageThreadsEntitlement.class);
    }

    private void checkEntitlementPresent(Class<?> callerClass, Class<? extends Entitlement> entitlementClass) {
        var requestingClass = requestingClass(callerClass);
        if (isTriviallyAllowed(requestingClass)) {
            return;
        }
        checkFlagEntitlement(getEntitlements(requestingClass), entitlementClass, requestingClass);
    }

    ModuleEntitlements getEntitlements(Class<?> requestingClass) {
        return moduleEntitlementsMap.computeIfAbsent(requestingClass.getModule(), m -> computeEntitlements(requestingClass));
    }

    private ModuleEntitlements computeEntitlements(Class<?> requestingClass) {
        Module requestingModule = requestingClass.getModule();
        if (isServerModule(requestingModule)) {
            return getModuleScopeEntitlements(serverEntitlements, requestingModule.getName(), SERVER_COMPONENT_NAME);
        }

        // plugins
        var pluginName = pluginResolver.apply(requestingClass);
        if (pluginName != null) {
            var pluginEntitlements = pluginsEntitlements.get(pluginName);
            if (pluginEntitlements == null) {
                return defaultEntitlements(pluginName);
            } else {
                final String scopeName;
                if (requestingModule.isNamed() == false) {
                    scopeName = ALL_UNNAMED;
                } else {
                    scopeName = requestingModule.getName();
                }
                return getModuleScopeEntitlements(pluginEntitlements, scopeName, pluginName);
            }
        }

        if (requestingModule.isNamed() == false && requestingClass.getPackageName().startsWith(apmAgentPackageName)) {
            // The APM agent is the only thing running non-modular in the system classloader
            return policyEntitlements(APM_AGENT_COMPONENT_NAME, apmAgentEntitlements);
        }

        return defaultEntitlements(UNKNOWN_COMPONENT_NAME);
    }

    private ModuleEntitlements getModuleScopeEntitlements(
        Map<String, List<Entitlement>> scopeEntitlements,
        String moduleName,
        String componentName
    ) {
        var entitlements = scopeEntitlements.get(moduleName);
        if (entitlements == null) {
            return defaultEntitlements(componentName);
        }
        return policyEntitlements(componentName, entitlements);
    }

    private static boolean isServerModule(Module requestingModule) {
        return requestingModule.isNamed() && requestingModule.getLayer() == ModuleLayer.boot();
    }

    /**
     * Walks the stack to determine which class should be checked for entitlements.
     *
     * @param callerClass when non-null will be returned;
     *                    this is a fast-path check that can avoid the stack walk
     *                    in cases where the caller class is available.
     * @return the requesting class, or {@code null} if the entire call stack
     * comes from the entitlement library itself.
     */
    Class<?> requestingClass(Class<?> callerClass) {
        if (callerClass != null) {
            // fast path
            return callerClass;
        }
        Optional<Class<?>> result = StackWalker.getInstance(RETAIN_CLASS_REFERENCE)
            .walk(frames -> findRequestingClass(frames.map(StackFrame::getDeclaringClass)));
        return result.orElse(null);
    }

    /**
     * Given a stream of classes corresponding to the frames from a {@link StackWalker},
     * returns the module whose entitlements should be checked.
     *
     * @throws NullPointerException if the requesting module is {@code null}
     */
    Optional<Class<?>> findRequestingClass(Stream<Class<?>> classes) {
        return classes.filter(c -> c.getModule() != entitlementsModule)  // Ignore the entitlements library
            .skip(1)                                           // Skip the sensitive caller method
            .findFirst();
    }

    /**
     * @return true if permission is granted regardless of the entitlement
     */
    private static boolean isTriviallyAllowed(Class<?> requestingClass) {
        if (logger.isTraceEnabled()) {
            logger.trace("Stack trace for upcoming trivially-allowed check", new Exception());
        }
        if (requestingClass == null) {
            logger.debug("Entitlement trivially allowed: no caller frames outside the entitlement library");
            return true;
        }
        if (systemModules.contains(requestingClass.getModule())) {
            logger.debug("Entitlement trivially allowed from system module [{}]", requestingClass.getModule().getName());
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
