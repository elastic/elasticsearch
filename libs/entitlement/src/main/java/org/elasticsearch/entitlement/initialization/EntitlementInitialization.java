/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.internal.provider.ProviderLocator;
import org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.instrumentation.Transformer;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.FileAccessTree;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.channels.spi.SelectorProvider;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.CONFIG;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.LIB;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.MODULES;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.PLUGINS;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ_WRITE;

/**
 * Called by the agent during {@code agentmain} to configure the entitlement system,
 * instantiate and configure an {@link EntitlementChecker},
 * make it available to the bootstrap library via {@link #checker()},
 * and then install the {@link org.elasticsearch.entitlement.instrumentation.Instrumenter}
 * to begin injecting our instrumentation.
 */
public class EntitlementInitialization {

    private static final Module ENTITLEMENTS_MODULE = PolicyManager.class.getModule();

    private static ElasticsearchEntitlementChecker manager;

    interface InstrumentationInfoFactory {
        InstrumentationService.InstrumentationInfo of(String methodName, Class<?>... parameterTypes) throws ClassNotFoundException,
            NoSuchMethodException;
    }

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return manager;
    }

    /**
     * Initializes the Entitlement system:
     * <ol>
     * <li>
     * Finds the version-specific subclass of {@link EntitlementChecker} to use
     * </li>
     * <li>
     * Builds the set of methods to instrument using {@link InstrumentationService#lookupMethods}
     * </li>
     * <li>
     * Augment this set “dynamically” using {@link InstrumentationService#lookupImplementationMethod}
     * </li>
     * <li>
     * Creates an {@link Instrumenter} via {@link InstrumentationService#newInstrumenter}, and adds a new {@link Transformer} (derived from
     * {@link java.lang.instrument.ClassFileTransformer}) that uses it. Transformers are invoked when a class is about to load, after its
     * bytes have been deserialized to memory but before the class is initialized.
     * </li>
     * <li>
     * Re-transforms all already loaded classes: we force the {@link Instrumenter} to run on classes that might have been already loaded
     * before entitlement initialization by calling the {@link java.lang.instrument.Instrumentation#retransformClasses} method on all
     * classes that were already loaded.
     * </li>
     * </ol>
     * <p>
     * The third step is needed as the JDK exposes some API through interfaces that have different (internal) implementations
     * depending on the JVM host platform. As we cannot instrument an interfaces, we find its concrete implementation.
     * A prime example is {@link FileSystemProvider}, which has different implementations (e.g. {@code UnixFileSystemProvider} or
     * {@code WindowsFileSystemProvider}). At runtime, we find the implementation class which is currently used by the JVM, and add
     * its methods to the set of methods to instrument. See e.g. {@link EntitlementInitialization#fileSystemProviderChecks}.
     * </p>
     * <p>
     * <strong>NOTE:</strong> this method is referenced by the agent reflectively
     * </p>
     *
     * @param inst the JVM instrumentation class instance
     */
    public static void initialize(Instrumentation inst) throws Exception {
        manager = initChecker();

        var latestCheckerInterface = getVersionSpecificCheckerClass(EntitlementChecker.class);
        var verifyBytecode = Booleans.parseBoolean(System.getProperty("es.entitlements.verify_bytecode", "false"));

        if (verifyBytecode) {
            ensureClassesSensitiveToVerificationAreInitialized();
        }

        Map<MethodKey, CheckMethod> checkMethods = new HashMap<>(INSTRUMENTATION_SERVICE.lookupMethods(latestCheckerInterface));
        Stream.of(
            fileSystemProviderChecks(),
            fileStoreChecks(),
            pathChecks(),
            Stream.of(
                INSTRUMENTATION_SERVICE.lookupImplementationMethod(
                    SelectorProvider.class,
                    "inheritedChannel",
                    SelectorProvider.provider().getClass(),
                    EntitlementChecker.class,
                    "checkSelectorProviderInheritedChannel"
                )
            )
        )
            .flatMap(Function.identity())
            .forEach(instrumentation -> checkMethods.put(instrumentation.targetMethod(), instrumentation.checkMethod()));

        var classesToTransform = checkMethods.keySet().stream().map(MethodKey::className).collect(Collectors.toSet());

        Instrumenter instrumenter = INSTRUMENTATION_SERVICE.newInstrumenter(latestCheckerInterface, checkMethods);
        var transformer = new Transformer(instrumenter, classesToTransform, verifyBytecode);
        inst.addTransformer(transformer, true);

        var classesToRetransform = findClassesToRetransform(inst.getAllLoadedClasses(), classesToTransform);
        try {
            inst.retransformClasses(classesToRetransform);
        } catch (VerifyError e) {
            // Turn on verification and try to retransform one class at the time to get detailed diagnostic
            transformer.enableClassVerification();

            for (var classToRetransform : classesToRetransform) {
                inst.retransformClasses(classToRetransform);
            }

            // We should have failed already in the loop above, but just in case we did not, rethrow.
            throw e;
        }
    }

    private static Class<?>[] findClassesToRetransform(Class<?>[] loadedClasses, Set<String> classesToTransform) {
        List<Class<?>> retransform = new ArrayList<>();
        for (Class<?> loadedClass : loadedClasses) {
            if (classesToTransform.contains(loadedClass.getName().replace(".", "/"))) {
                retransform.add(loadedClass);
            }
        }
        return retransform.toArray(new Class<?>[0]);
    }

    private static PolicyManager createPolicyManager() {
        EntitlementBootstrap.BootstrapArgs bootstrapArgs = EntitlementBootstrap.bootstrapArgs();
        Map<String, Policy> pluginPolicies = bootstrapArgs.pluginPolicies();
        PathLookup pathLookup = bootstrapArgs.pathLookup();

        validateFilesEntitlements(pluginPolicies, pathLookup);

        return new PolicyManager(
            HardcodedEntitlements.serverPolicy(pathLookup.pidFile(), bootstrapArgs.serverPolicyPatch()),
            HardcodedEntitlements.agentEntitlements(),
            pluginPolicies,
            EntitlementBootstrap.bootstrapArgs().scopeResolver(),
            EntitlementBootstrap.bootstrapArgs().sourcePaths(),
            ENTITLEMENTS_MODULE,
            pathLookup,
            bootstrapArgs.suppressFailureLogClasses()
        );
    }

    // package visible for tests
    static void validateFilesEntitlements(Map<String, Policy> pluginPolicies, PathLookup pathLookup) {
        Set<Path> readAccessForbidden = new HashSet<>();
        pathLookup.getBaseDirPaths(PLUGINS).forEach(p -> readAccessForbidden.add(p.toAbsolutePath().normalize()));
        pathLookup.getBaseDirPaths(MODULES).forEach(p -> readAccessForbidden.add(p.toAbsolutePath().normalize()));
        pathLookup.getBaseDirPaths(LIB).forEach(p -> readAccessForbidden.add(p.toAbsolutePath().normalize()));
        Set<Path> writeAccessForbidden = new HashSet<>();
        pathLookup.getBaseDirPaths(CONFIG).forEach(p -> writeAccessForbidden.add(p.toAbsolutePath().normalize()));
        for (var pluginPolicy : pluginPolicies.entrySet()) {
            for (var scope : pluginPolicy.getValue().scopes()) {
                var filesEntitlement = scope.entitlements()
                    .stream()
                    .filter(x -> x instanceof FilesEntitlement)
                    .map(x -> ((FilesEntitlement) x))
                    .findFirst();
                if (filesEntitlement.isPresent()) {
                    var fileAccessTree = FileAccessTree.withoutExclusivePaths(filesEntitlement.get(), pathLookup, null);
                    validateReadFilesEntitlements(pluginPolicy.getKey(), scope.moduleName(), fileAccessTree, readAccessForbidden);
                    validateWriteFilesEntitlements(pluginPolicy.getKey(), scope.moduleName(), fileAccessTree, writeAccessForbidden);
                }
            }
        }
    }

    private static IllegalArgumentException buildValidationException(
        String componentName,
        String moduleName,
        Path forbiddenPath,
        FilesEntitlement.Mode mode
    ) {
        return new IllegalArgumentException(
            Strings.format(
                "policy for module [%s] in [%s] has an invalid file entitlement. Any path under [%s] is forbidden for mode [%s].",
                moduleName,
                componentName,
                forbiddenPath,
                mode
            )
        );
    }

    private static void validateReadFilesEntitlements(
        String componentName,
        String moduleName,
        FileAccessTree fileAccessTree,
        Set<Path> readForbiddenPaths
    ) {

        for (Path forbiddenPath : readForbiddenPaths) {
            if (fileAccessTree.canRead(forbiddenPath)) {
                throw buildValidationException(componentName, moduleName, forbiddenPath, READ);
            }
        }
    }

    private static void validateWriteFilesEntitlements(
        String componentName,
        String moduleName,
        FileAccessTree fileAccessTree,
        Set<Path> writeForbiddenPaths
    ) {
        for (Path forbiddenPath : writeForbiddenPaths) {
            if (fileAccessTree.canWrite(forbiddenPath)) {
                throw buildValidationException(componentName, moduleName, forbiddenPath, READ_WRITE);
            }
        }
    }

    private static Path getUserHome() {
        String userHome = System.getProperty("user.home");
        if (userHome == null) {
            throw new IllegalStateException("user.home system property is required");
        }
        return PathUtils.get(userHome);
    }

    private static Stream<InstrumentationService.InstrumentationInfo> fileSystemProviderChecks() throws ClassNotFoundException,
        NoSuchMethodException {
        var fileSystemProviderClass = FileSystems.getDefault().provider().getClass();

        var instrumentation = new InstrumentationInfoFactory() {
            @Override
            public InstrumentationService.InstrumentationInfo of(String methodName, Class<?>... parameterTypes)
                throws ClassNotFoundException, NoSuchMethodException {
                return INSTRUMENTATION_SERVICE.lookupImplementationMethod(
                    FileSystemProvider.class,
                    methodName,
                    fileSystemProviderClass,
                    EntitlementChecker.class,
                    "check" + Character.toUpperCase(methodName.charAt(0)) + methodName.substring(1),
                    parameterTypes
                );
            }
        };

        return Stream.of(
            instrumentation.of("newFileSystem", URI.class, Map.class),
            instrumentation.of("newFileSystem", Path.class, Map.class),
            instrumentation.of("newInputStream", Path.class, OpenOption[].class),
            instrumentation.of("newOutputStream", Path.class, OpenOption[].class),
            instrumentation.of("newFileChannel", Path.class, Set.class, FileAttribute[].class),
            instrumentation.of("newAsynchronousFileChannel", Path.class, Set.class, ExecutorService.class, FileAttribute[].class),
            instrumentation.of("newByteChannel", Path.class, Set.class, FileAttribute[].class),
            instrumentation.of("newDirectoryStream", Path.class, DirectoryStream.Filter.class),
            instrumentation.of("createDirectory", Path.class, FileAttribute[].class),
            instrumentation.of("createSymbolicLink", Path.class, Path.class, FileAttribute[].class),
            instrumentation.of("createLink", Path.class, Path.class),
            instrumentation.of("delete", Path.class),
            instrumentation.of("deleteIfExists", Path.class),
            instrumentation.of("readSymbolicLink", Path.class),
            instrumentation.of("copy", Path.class, Path.class, CopyOption[].class),
            instrumentation.of("move", Path.class, Path.class, CopyOption[].class),
            instrumentation.of("isSameFile", Path.class, Path.class),
            instrumentation.of("isHidden", Path.class),
            instrumentation.of("getFileStore", Path.class),
            instrumentation.of("checkAccess", Path.class, AccessMode[].class),
            instrumentation.of("getFileAttributeView", Path.class, Class.class, LinkOption[].class),
            instrumentation.of("readAttributes", Path.class, Class.class, LinkOption[].class),
            instrumentation.of("readAttributes", Path.class, String.class, LinkOption[].class),
            instrumentation.of("readAttributesIfExists", Path.class, Class.class, LinkOption[].class),
            instrumentation.of("setAttribute", Path.class, String.class, Object.class, LinkOption[].class),
            instrumentation.of("exists", Path.class, LinkOption[].class)
        );
    }

    private static Stream<InstrumentationService.InstrumentationInfo> fileStoreChecks() {
        var fileStoreClasses = StreamSupport.stream(FileSystems.getDefault().getFileStores().spliterator(), false)
            .map(FileStore::getClass)
            .distinct();
        return fileStoreClasses.flatMap(fileStoreClass -> {
            var instrumentation = new InstrumentationInfoFactory() {
                @Override
                public InstrumentationService.InstrumentationInfo of(String methodName, Class<?>... parameterTypes)
                    throws ClassNotFoundException, NoSuchMethodException {
                    return INSTRUMENTATION_SERVICE.lookupImplementationMethod(
                        FileStore.class,
                        methodName,
                        fileStoreClass,
                        EntitlementChecker.class,
                        "check" + Character.toUpperCase(methodName.charAt(0)) + methodName.substring(1),
                        parameterTypes
                    );
                }
            };

            try {
                return Stream.of(
                    instrumentation.of("getFileStoreAttributeView", Class.class),
                    instrumentation.of("getAttribute", String.class),
                    instrumentation.of("getBlockSize"),
                    instrumentation.of("getTotalSpace"),
                    instrumentation.of("getUnallocatedSpace"),
                    instrumentation.of("getUsableSpace"),
                    instrumentation.of("isReadOnly"),
                    instrumentation.of("name"),
                    instrumentation.of("type")

                );
            } catch (NoSuchMethodException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static Stream<InstrumentationService.InstrumentationInfo> pathChecks() {
        var pathClasses = StreamSupport.stream(FileSystems.getDefault().getRootDirectories().spliterator(), false)
            .map(Path::getClass)
            .distinct();
        return pathClasses.flatMap(pathClass -> {
            InstrumentationInfoFactory instrumentation = (String methodName, Class<?>... parameterTypes) -> INSTRUMENTATION_SERVICE
                .lookupImplementationMethod(
                    Path.class,
                    methodName,
                    pathClass,
                    EntitlementChecker.class,
                    "checkPath" + Character.toUpperCase(methodName.charAt(0)) + methodName.substring(1),
                    parameterTypes
                );

            try {
                return Stream.of(
                    instrumentation.of("toRealPath", LinkOption[].class),
                    instrumentation.of("register", WatchService.class, WatchEvent.Kind[].class),
                    instrumentation.of("register", WatchService.class, WatchEvent.Kind[].class, WatchEvent.Modifier[].class)
                );
            } catch (NoSuchMethodException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * If bytecode verification is enabled, ensure these classes get loaded before transforming/retransforming them.
     * For these classes, the order in which we transform and verify them matters. Verification during class transformation is at least an
     * unforeseen (if not unsupported) scenario: we are loading a class, and while we are still loading it (during transformation) we try
     * to verify it. This in turn leads to more classes loading (for verification purposes), which could turn into those classes to be
     * transformed and undergo verification. In order to avoid circularity errors as much as possible, we force a partial order.
     */
    private static void ensureClassesSensitiveToVerificationAreInitialized() {
        var classesToInitialize = Set.of("sun.net.www.protocol.http.HttpURLConnection");
        for (String className : classesToInitialize) {
            try {
                Class.forName(className);
            } catch (ClassNotFoundException unexpected) {
                throw new AssertionError(unexpected);
            }
        }
    }

    /**
     * Returns the "most recent" checker class compatible with the current runtime Java version.
     * For checkers, we have (optionally) version specific classes, each with a prefix (e.g. Java23).
     * The mapping cannot be automatic, as it depends on the actual presence of these classes in the final Jar (see
     * the various mainXX source sets).
     */
    private static Class<?> getVersionSpecificCheckerClass(Class<?> baseClass) {
        String packageName = baseClass.getPackageName();
        String baseClassName = baseClass.getSimpleName();
        int javaVersion = Runtime.version().feature();

        final String classNamePrefix;
        if (javaVersion >= 23) {
            // All Java version from 23 onwards will be able to use che checks in the Java23EntitlementChecker interface and implementation
            classNamePrefix = "Java23";
        } else {
            // For any other Java version, the basic EntitlementChecker interface and implementation contains all the supported checks
            classNamePrefix = "";
        }
        final String className = packageName + "." + classNamePrefix + baseClassName;
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("entitlement lib cannot find entitlement class " + className, e);
        }
        return clazz;
    }

    private static ElasticsearchEntitlementChecker initChecker() {
        final PolicyManager policyManager = createPolicyManager();

        final Class<?> clazz = getVersionSpecificCheckerClass(ElasticsearchEntitlementChecker.class);

        Constructor<?> constructor;
        try {
            constructor = clazz.getConstructor(PolicyManager.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError("entitlement impl is missing no arg constructor", e);
        }
        try {
            return (ElasticsearchEntitlementChecker) constructor.newInstance(policyManager);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new AssertionError(e);
        }
    }

    private static final InstrumentationService INSTRUMENTATION_SERVICE = new ProviderLocator<>(
        "entitlement",
        InstrumentationService.class,
        "org.elasticsearch.entitlement.instrumentation",
        Set.of()
    ).get();
}
