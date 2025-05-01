/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.entitlement.runtime.policy.FileAccessTree.ExclusiveFileEntitlement;
import org.elasticsearch.entitlement.runtime.policy.FileAccessTree.ExclusivePath;
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
import java.io.IOException;
import java.lang.StackWalker.StackFrame;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import static java.util.zip.ZipFile.OPEN_DELETE;
import static java.util.zip.ZipFile.OPEN_READ;
import static org.elasticsearch.entitlement.bridge.Util.NO_CLASS;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.TEMP;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.APM_AGENT;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.PLUGIN;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.SERVER;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.UNKNOWN;

/**
 * This class is responsible for finding the <strong>component</strong> (system, server, plugin, agent) for a caller class to check,
 * retrieve the policy and entitlements for that component, and check them against the action(s) the caller wants to perform.
 * <p>
 * To find a component:
 * <ul>
 * <li>
 * For plugins, we use the Module -> Plugin name (String) passed to the ctor
 * </li>
 * <li>
 * For the system component, we build a set ({@link PolicyManager#SYSTEM_LAYER_MODULES}) of references to modules that belong that
 * component, i.e. the component containing what we consider system modules. These are the modules that:
 * <ul>
 * <li>
 * are in the boot module layer ({@link ModuleLayer#boot()});
 * </li>
 * <li>
 * are defined in {@link ModuleFinder#ofSystem()};
 * </li>
 * <li>
 * are not in the ({@link PolicyManager#MODULES_EXCLUDED_FROM_SYSTEM_MODULES}) (currently: {@code java.desktop})
 * </li>
 * </ul>
 * </li>
 * <li>
 * For the server component, we build a set ({@link PolicyManager#SERVER_LAYER_MODULES}) as the set of modules that are in the boot module
 * layer but not in the system component.
 * </li>
 * </ul>
 * <p>
 * When a check is performed (e.g. {@link PolicyManager#checkExitVM(Class)}, we get the module the caller class belongs to via
 * {@link Class#getModule} and try (in order) to see if that class belongs to:
 * <ol>
 * <li>
 * The system component - if a module is contained in {@link PolicyManager#SYSTEM_LAYER_MODULES}
 * </li>
 * <li>
 * The server component - if a module is contained in {@link PolicyManager#SERVER_LAYER_MODULES}
 * </li>
 * <li>
 * One of the plugins or modules - if the module is present in the {@code PluginsResolver} map
 * </li>
 * <li>
 * A known agent (APM)
 * </li>
 * <li>
 * Something else
 * </li>
 * </ol>
 * <p>
 * Once it has a component, this class maps it to a policy and check the action performed by the caller class against its entitlements,
 * either allowing it to proceed or raising a {@link NotEntitledException} if the caller class is not entitled to perform the action.
 * </p>
 * <p>
 * All these methods start in the same way: the components identified in the previous section are used to establish if and how to check:
 * If the caller class belongs to {@link PolicyManager#SYSTEM_LAYER_MODULES}, no check is performed (the call is trivially allowed, see
 * {@link PolicyManager#isTriviallyAllowed}).
 * Otherwise, we lazily compute and create a {@link PolicyManager.ModuleEntitlements} record (see
 * {@link PolicyManager#computeEntitlements}). The record is cached so it can be used in following checks, stored in a
 * {@code Module -> ModuleEntitlement} map.
 * </p>
 */
public class PolicyManager {
    /**
     * Use this if you don't have a {@link ModuleEntitlements} in hand.
     */
    private static final Logger generalLogger = LogManager.getLogger(PolicyManager.class);

    static final Class<?> DEFAULT_FILESYSTEM_CLASS = PathUtils.getDefaultFileSystem().getClass();

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
    record ModuleEntitlements(
        String componentName,
        Map<Class<? extends Entitlement>, List<Entitlement>> entitlementsByType,
        FileAccessTree fileAccess,
        Logger logger
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

    private FileAccessTree getDefaultFileAccess(Path componentPath) {
        return FileAccessTree.withoutExclusivePaths(FilesEntitlement.EMPTY, pathLookup, componentPath);
    }

    // pkg private for testing
    ModuleEntitlements defaultEntitlements(String componentName, Path componentPath, String moduleName) {
        return new ModuleEntitlements(componentName, Map.of(), getDefaultFileAccess(componentPath), getLogger(componentName, moduleName));
    }

    // pkg private for testing
    ModuleEntitlements policyEntitlements(String componentName, Path componentPath, String moduleName, List<Entitlement> entitlements) {
        FilesEntitlement filesEntitlement = FilesEntitlement.EMPTY;
        for (Entitlement entitlement : entitlements) {
            if (entitlement instanceof FilesEntitlement) {
                filesEntitlement = (FilesEntitlement) entitlement;
            }
        }
        return new ModuleEntitlements(
            componentName,
            entitlements.stream().collect(groupingBy(Entitlement::getClass)),
            FileAccessTree.of(componentName, moduleName, filesEntitlement, pathLookup, componentPath, exclusivePaths),
            getLogger(componentName, moduleName)
        );
    }

    final Map<Module, ModuleEntitlements> moduleEntitlementsMap = new ConcurrentHashMap<>();

    private final Map<String, List<Entitlement>> serverEntitlements;
    private final List<Entitlement> apmAgentEntitlements;
    private final Map<String, Map<String, List<Entitlement>>> pluginsEntitlements;
    private final Function<Class<?>, PolicyScope> scopeResolver;
    private final PathLookup pathLookup;
    private final Set<Class<?>> mutedClasses;

    public static final String ALL_UNNAMED = "ALL-UNNAMED";

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

    private final Map<String, Path> sourcePaths;

    /**
     * Frames originating from this module are ignored in the permission logic.
     */
    private final Module entitlementsModule;

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
        Map<String, Path> sourcePaths,
        Module entitlementsModule,
        PathLookup pathLookup,
        Set<Class<?>> suppressFailureLogClasses
    ) {
        this.serverEntitlements = buildScopeEntitlementsMap(requireNonNull(serverPolicy));
        this.apmAgentEntitlements = apmAgentEntitlements;
        this.pluginsEntitlements = requireNonNull(pluginPolicies).entrySet()
            .stream()
            .collect(toUnmodifiableMap(Map.Entry::getKey, e -> buildScopeEntitlementsMap(e.getValue())));
        this.scopeResolver = scopeResolver;
        this.sourcePaths = sourcePaths;
        this.entitlementsModule = entitlementsModule;
        this.pathLookup = requireNonNull(pathLookup);
        this.mutedClasses = suppressFailureLogClasses;

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

        ModuleEntitlements entitlements = getEntitlements(requestingClass);
        notEntitled(
            Strings.format(
                "component [%s], module [%s], class [%s], operation [%s]",
                entitlements.componentName(),
                getModuleName(requestingClass),
                requestingClass,
                operationDescription.get()
            ),
            callerClass,
            entitlements
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

    public void checkLoggingFileHandler(Class<?> callerClass) {
        neverEntitled(callerClass, () -> walkStackForCheckMethodName().orElse("create logging file handler"));
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

    /**
     * Check for operations that can modify the way file operations are handled
     */
    public void checkChangeFilesHandling(Class<?> callerClass) {
        checkChangeJVMGlobalState(callerClass);
    }

    @SuppressForbidden(reason = "Explicitly checking File apis")
    public void checkFileRead(Class<?> callerClass, File file) {
        checkFileRead(callerClass, file.toPath());
    }

    private static boolean isPathOnDefaultFilesystem(Path path) {
        var pathFileSystemClass = path.getFileSystem().getClass();
        if (path.getFileSystem().getClass() != DEFAULT_FILESYSTEM_CLASS) {
            generalLogger.trace(
                () -> Strings.format(
                    "File entitlement trivially allowed: path [%s] is for a different FileSystem class [%s], default is [%s]",
                    path.toString(),
                    pathFileSystemClass.getName(),
                    DEFAULT_FILESYSTEM_CLASS.getName()
                )
            );
            return false;
        }
        return true;
    }

    public void checkFileRead(Class<?> callerClass, Path path) {
        try {
            checkFileRead(callerClass, path, false);
        } catch (NoSuchFileException e) {
            assert false : "NoSuchFileException should only be thrown when following links";
            var notEntitledException = new NotEntitledException(e.getMessage());
            notEntitledException.addSuppressed(e);
            throw notEntitledException;
        }
    }

    public void checkFileRead(Class<?> callerClass, Path path, boolean followLinks) throws NoSuchFileException {
        if (isPathOnDefaultFilesystem(path) == false) {
            return;
        }
        var requestingClass = requestingClass(callerClass);
        if (isTriviallyAllowed(requestingClass)) {
            return;
        }

        ModuleEntitlements entitlements = getEntitlements(requestingClass);

        Path realPath = null;
        boolean canRead = entitlements.fileAccess().canRead(path);
        if (canRead && followLinks) {
            try {
                realPath = path.toRealPath();
                if (realPath.equals(path) == false) {
                    canRead = entitlements.fileAccess().canRead(realPath);
                }
            } catch (NoSuchFileException e) {
                throw e; // rethrow
            } catch (IOException e) {
                canRead = false;
            }
        }

        if (canRead == false) {
            notEntitled(
                Strings.format(
                    "component [%s], module [%s], class [%s], entitlement [file], operation [read], path [%s]",
                    entitlements.componentName(),
                    getModuleName(requestingClass),
                    requestingClass,
                    realPath == null ? path : Strings.format("%s -> %s", path, realPath)
                ),
                callerClass,
                entitlements
            );
        }
    }

    @SuppressForbidden(reason = "Explicitly checking File apis")
    public void checkFileWrite(Class<?> callerClass, File file) {
        checkFileWrite(callerClass, file.toPath());
    }

    public void checkFileWrite(Class<?> callerClass, Path path) {
        if (isPathOnDefaultFilesystem(path) == false) {
            return;
        }
        var requestingClass = requestingClass(callerClass);
        if (isTriviallyAllowed(requestingClass)) {
            return;
        }

        ModuleEntitlements entitlements = getEntitlements(requestingClass);
        if (entitlements.fileAccess().canWrite(path) == false) {
            notEntitled(
                Strings.format(
                    "component [%s], module [%s], class [%s], entitlement [file], operation [write], path [%s]",
                    entitlements.componentName(),
                    getModuleName(requestingClass),
                    requestingClass,
                    path
                ),
                callerClass,
                entitlements
            );
        }
    }

    public void checkCreateTempFile(Class<?> callerClass) {
        // in production there should only ever be a single temp directory
        // so we can safely assume we only need to check the sole element in this stream
        checkFileWrite(callerClass, pathLookup.getBaseDirPaths(TEMP).findFirst().get());
    }

    @SuppressForbidden(reason = "Explicitly checking File apis")
    public void checkFileWithZipMode(Class<?> callerClass, File file, int zipMode) {
        assert zipMode == OPEN_READ || zipMode == (OPEN_READ | OPEN_DELETE);
        if ((zipMode & OPEN_DELETE) == OPEN_DELETE) {
            // This needs both read and write, but we happen to know that checkFileWrite
            // actually checks both.
            checkFileWrite(callerClass, file);
        } else {
            checkFileRead(callerClass, file);
        }
    }

    public void checkFileDescriptorRead(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "read file descriptor");
    }

    public void checkFileDescriptorWrite(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "write file descriptor");
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
        checkFlagEntitlement(classEntitlements, InboundNetworkEntitlement.class, requestingClass, callerClass);
        checkFlagEntitlement(classEntitlements, OutboundNetworkEntitlement.class, requestingClass, callerClass);
    }

    public void checkUnsupportedURLProtocolConnection(Class<?> callerClass, String protocol) {
        neverEntitled(callerClass, () -> Strings.format("unsupported URL protocol [%s]", protocol));
    }

    private void checkFlagEntitlement(
        ModuleEntitlements classEntitlements,
        Class<? extends Entitlement> entitlementClass,
        Class<?> requestingClass,
        Class<?> callerClass
    ) {
        if (classEntitlements.hasEntitlement(entitlementClass) == false) {
            notEntitled(
                Strings.format(
                    "component [%s], module [%s], class [%s], entitlement [%s]",
                    classEntitlements.componentName(),
                    getModuleName(requestingClass),
                    requestingClass,
                    PolicyParser.buildEntitlementNameFromClass(entitlementClass)
                ),
                callerClass,
                classEntitlements
            );
        }
        classEntitlements.logger()
            .debug(
                () -> Strings.format(
                    "Entitled: component [%s], module [%s], class [%s], entitlement [%s]",
                    classEntitlements.componentName(),
                    getModuleName(requestingClass),
                    requestingClass,
                    PolicyParser.buildEntitlementNameFromClass(entitlementClass)
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
            entitlements.logger()
                .debug(
                    () -> Strings.format(
                        "Entitled: component [%s], module [%s], class [%s], entitlement [write_system_properties], property [%s]",
                        entitlements.componentName(),
                        getModuleName(requestingClass),
                        requestingClass,
                        property
                    )
                );
            return;
        }
        notEntitled(
            Strings.format(
                "component [%s], module [%s], class [%s], entitlement [write_system_properties], property [%s]",
                entitlements.componentName(),
                getModuleName(requestingClass),
                requestingClass,
                property
            ),
            callerClass,
            entitlements
        );
    }

    private void notEntitled(String message, Class<?> callerClass, ModuleEntitlements entitlements) {
        var exception = new NotEntitledException(message);
        // Don't emit a log for muted classes, e.g. classes containing self tests
        if (mutedClasses.contains(callerClass) == false) {
            entitlements.logger().warn("Not entitled: {}", message, exception);
        }
        throw exception;
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

    public void checkManageThreadsEntitlement(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, ManageThreadsEntitlement.class);
    }

    private void checkEntitlementPresent(Class<?> callerClass, Class<? extends Entitlement> entitlementClass) {
        var requestingClass = requestingClass(callerClass);
        if (isTriviallyAllowed(requestingClass)) {
            return;
        }
        checkFlagEntitlement(getEntitlements(requestingClass), entitlementClass, requestingClass, callerClass);
    }

    ModuleEntitlements getEntitlements(Class<?> requestingClass) {
        return moduleEntitlementsMap.computeIfAbsent(requestingClass.getModule(), m -> computeEntitlements(requestingClass));
    }

    private ModuleEntitlements computeEntitlements(Class<?> requestingClass) {
        var policyScope = scopeResolver.apply(requestingClass);
        var componentName = policyScope.componentName();
        var moduleName = policyScope.moduleName();

        switch (policyScope.kind()) {
            case SERVER -> {
                return getModuleScopeEntitlements(
                    serverEntitlements,
                    moduleName,
                    SERVER.componentName,
                    getComponentPathFromClass(requestingClass)
                );
            }
            case APM_AGENT -> {
                // The APM agent is the only thing running non-modular in the system classloader
                return policyEntitlements(
                    APM_AGENT.componentName,
                    getComponentPathFromClass(requestingClass),
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
                    return defaultEntitlements(componentName, sourcePaths.get(componentName), moduleName);
                } else {
                    return getModuleScopeEntitlements(pluginEntitlements, moduleName, componentName, sourcePaths.get(componentName));
                }
            }
        }
    }

    // pkg private for testing
    static Path getComponentPathFromClass(Class<?> requestingClass) {
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
        Path componentPath
    ) {
        var entitlements = scopeEntitlements.get(scopeName);
        if (entitlements == null) {
            return defaultEntitlements(componentName, componentPath, scopeName);
        }
        return policyEntitlements(componentName, componentPath, scopeName, entitlements);
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
            .walk(frames -> findRequestingFrame(frames).map(StackFrame::getDeclaringClass));
        return result.orElse(null);
    }

    /**
     * Given a stream of {@link StackFrame}s, identify the one whose entitlements should be checked.
     */
    Optional<StackFrame> findRequestingFrame(Stream<StackFrame> frames) {
        return frames.filter(f -> f.getDeclaringClass().getModule() != entitlementsModule) // ignore entitlements library
            .skip(1) // Skip the sensitive caller method
            .findFirst();
    }

    /**
     * @return true if permission is granted regardless of the entitlement
     */
    private static boolean isTriviallyAllowed(Class<?> requestingClass) {
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
        if (SYSTEM_LAYER_MODULES.contains(requestingClass.getModule())) {
            generalLogger.debug("Entitlement trivially allowed from system module [{}]", requestingClass.getModule().getName());
            return true;
        }
        generalLogger.trace("Entitlement not trivially allowed");
        return false;
    }

    /**
     * @return the {@code requestingClass}'s module name as it would appear in an entitlement policy file
     */
    private static String getModuleName(Class<?> requestingClass) {
        String name = requestingClass.getModule().getName();
        return (name == null) ? ALL_UNNAMED : name;
    }

    @Override
    public String toString() {
        return "PolicyManager{" + "serverEntitlements=" + serverEntitlements + ", pluginsEntitlements=" + pluginsEntitlements + '}';
    }
}
