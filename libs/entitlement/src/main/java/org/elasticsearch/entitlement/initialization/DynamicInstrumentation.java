/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.core.internal.provider.ProviderLocator;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.instrumentation.Transformer;

import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class DynamicInstrumentation {

    interface InstrumentationInfoFactory {
        InstrumentationService.InstrumentationInfo of(String methodName, Class<?>... parameterTypes) throws ClassNotFoundException,
            NoSuchMethodException;
    }

    private static final InstrumentationService INSTRUMENTATION_SERVICE = new ProviderLocator<>(
        "entitlement",
        InstrumentationService.class,
        "org.elasticsearch.entitlement.instrumentation",
        Set.of()
    ).get();

    /**
     * Initializes the dynamic (agent-based) instrumentation:
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
     * its methods to the set of methods to instrument. See e.g. {@link DynamicInstrumentation#fileSystemProviderChecks}.
     * </p>
     *
     * @param inst             the JVM instrumentation class instance
     * @param checkerInterface the interface to use to find methods to instrument and to use in the injected instrumentation code
     * @param verifyBytecode   whether we should perform bytecode verification before and after instrumenting each method
     */
    static void initialize(Instrumentation inst, Class<?> checkerInterface, boolean verifyBytecode) throws ClassNotFoundException,
        NoSuchMethodException, UnmodifiableClassException {

        var checkMethods = getMethodsToInstrument(checkerInterface);
        var classesToTransform = checkMethods.keySet().stream().map(MethodKey::className).collect(Collectors.toSet());

        Instrumenter instrumenter = INSTRUMENTATION_SERVICE.newInstrumenter(checkerInterface, checkMethods);
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

    private static Map<MethodKey, CheckMethod> getMethodsToInstrument(Class<?> checkerInterface) throws ClassNotFoundException,
        NoSuchMethodException {
        Map<MethodKey, CheckMethod> checkMethods = new HashMap<>(INSTRUMENTATION_SERVICE.lookupMethods(checkerInterface));
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

        return checkMethods;
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

        var allVersionsMethods = Stream.of(
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
            instrumentation.of("setAttribute", Path.class, String.class, Object.class, LinkOption[].class)
        );

        if (Runtime.version().feature() >= 20) {
            var java20EntitlementCheckerClass = EntitlementCheckerUtils.getVersionSpecificCheckerClass(EntitlementChecker.class, 20);
            var java20Methods = Stream.of(
                INSTRUMENTATION_SERVICE.lookupImplementationMethod(
                    FileSystemProvider.class,
                    "readAttributesIfExists",
                    fileSystemProviderClass,
                    java20EntitlementCheckerClass,
                    "checkReadAttributesIfExists",
                    Path.class,
                    Class.class,
                    LinkOption[].class
                ),
                INSTRUMENTATION_SERVICE.lookupImplementationMethod(
                    FileSystemProvider.class,
                    "exists",
                    fileSystemProviderClass,
                    java20EntitlementCheckerClass,
                    "checkExists",
                    Path.class,
                    LinkOption[].class
                )
            );
            return Stream.concat(allVersionsMethods, java20Methods);
        }
        return allVersionsMethods;
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

    private static Class<?>[] findClassesToRetransform(Class<?>[] loadedClasses, Set<String> classesToTransform) {
        List<Class<?>> retransform = new ArrayList<>();
        for (Class<?> loadedClass : loadedClasses) {
            if (classesToTransform.contains(loadedClass.getName().replace(".", "/"))) {
                retransform.add(loadedClass);
            }
        }
        return retransform.toArray(new Class<?>[0]);
    }
}
