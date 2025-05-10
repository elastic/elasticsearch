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
import org.elasticsearch.entitlement.runtime.policy.PolicyManager.ModuleEntitlements;
import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ManageThreadsEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ReadStoreAttributesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.SetHttpsConnectionPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;
import static java.util.function.Predicate.not;
import static java.util.zip.ZipFile.OPEN_DELETE;
import static java.util.zip.ZipFile.OPEN_READ;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.TEMP;

/**
 * Connects the {@link PolicyChecker} interface to a {@link PolicyManager}
 * to perform the checks in accordance with the policy.
 * Determines the caller class, queries {@link PolicyManager}
 * to find what entitlements have been granted to that class,
 * and finally checks whether the desired entitlements are present.
 */
public class PolicyCheckerImpl implements PolicyChecker {
    static final Class<?> DEFAULT_FILESYSTEM_CLASS = PathUtils.getDefaultFileSystem().getClass();
    protected final Set<Class<?>> mutedClasses;
    /**
     * Frames originating from this module are ignored in the permission logic.
     */
    protected final Module entitlementsModule;

    private final PolicyManager policyManager;

    private final PathLookup pathLookup;

    public PolicyCheckerImpl(
        Set<Class<?>> suppressFailureLogClasses,
        Module entitlementsModule,
        PolicyManager policyManager,
        PathLookup pathLookup
    ) {
        this.mutedClasses = suppressFailureLogClasses;
        this.entitlementsModule = entitlementsModule;
        this.policyManager = policyManager;
        this.pathLookup = pathLookup;
    }

    private static boolean isPathOnDefaultFilesystem(Path path) {
        var pathFileSystemClass = path.getFileSystem().getClass();
        if (path.getFileSystem().getClass() != DEFAULT_FILESYSTEM_CLASS) {
            PolicyManager.generalLogger.trace(
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

    /**
     * @return the {@code requestingClass}'s module name as it would appear in an entitlement policy file
     */
    private static String getModuleName(Class<?> requestingClass) {
        String name = requestingClass.getModule().getName();
        return (name == null) ? PolicyManager.ALL_UNNAMED : name;
    }

    @Override
    public void checkStartProcess(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "start process");
    }

    @Override
    public void checkWriteStoreAttributes(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "change file store attributes");
    }

    @Override
    public void checkReadStoreAttributes(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, ReadStoreAttributesEntitlement.class);
    }

    /**
     * @param operationDescription is only called when the operation is not trivially allowed, meaning the check is about to fail;
     *                            therefore, its performance is not a major concern.
     */
    private void neverEntitled(Class<?> callerClass, Supplier<String> operationDescription) {
        var requestingClass = requestingClass(callerClass);
        if (policyManager.isTriviallyAllowed(requestingClass)) {
            return;
        }

        ModuleEntitlements entitlements = policyManager.getEntitlements(requestingClass);
        notEntitled(
            Strings.format(
                "component [%s], module [%s], class [%s], operation [%s]",
                entitlements.componentName(),
                PolicyCheckerImpl.getModuleName(requestingClass),
                requestingClass,
                operationDescription.get()
            ),
            callerClass,
            entitlements
        );
    }

    @Override
    public void checkExitVM(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, ExitVMEntitlement.class);
    }

    @Override
    public void checkCreateClassLoader(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, CreateClassLoaderEntitlement.class);
    }

    @Override
    public void checkSetHttpsConnectionProperties(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, SetHttpsConnectionPropertiesEntitlement.class);
    }

    @Override
    public void checkChangeJVMGlobalState(Class<?> callerClass) {
        neverEntitled(callerClass, () -> walkStackForCheckMethodName().orElse("change JVM global state"));
    }

    @Override
    public void checkLoggingFileHandler(Class<?> callerClass) {
        neverEntitled(callerClass, () -> walkStackForCheckMethodName().orElse("create logging file handler"));
    }

    private Optional<String> walkStackForCheckMethodName() {
        // Look up the check$ method to compose an informative error message.
        // This way, we don't need to painstakingly describe every individual global-state change.
        return StackWalker.getInstance()
            .walk(
                frames -> frames.map(StackWalker.StackFrame::getMethodName)
                    .dropWhile(not(methodName -> methodName.startsWith(InstrumentationService.CHECK_METHOD_PREFIX)))
                    .findFirst()
            )
            .map(this::operationDescription);
    }

    /**
     * Check for operations that can modify the way network operations are handled
     */
    @Override
    public void checkChangeNetworkHandling(Class<?> callerClass) {
        checkChangeJVMGlobalState(callerClass);
    }

    /**
     * Check for operations that can modify the way file operations are handled
     */
    @Override
    public void checkChangeFilesHandling(Class<?> callerClass) {
        checkChangeJVMGlobalState(callerClass);
    }

    @SuppressForbidden(reason = "Explicitly checking File apis")
    @Override
    public void checkFileRead(Class<?> callerClass, File file) {
        checkFileRead(callerClass, file.toPath());
    }

    @Override
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

    @Override
    public void checkFileRead(Class<?> callerClass, Path path, boolean followLinks) throws NoSuchFileException {
        if (PolicyCheckerImpl.isPathOnDefaultFilesystem(path) == false) {
            return;
        }
        var requestingClass = requestingClass(callerClass);
        if (policyManager.isTriviallyAllowed(requestingClass)) {
            return;
        }

        ModuleEntitlements entitlements = policyManager.getEntitlements(requestingClass);

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
                    PolicyCheckerImpl.getModuleName(requestingClass),
                    requestingClass,
                    realPath == null ? path : Strings.format("%s -> %s", path, realPath)
                ),
                callerClass,
                entitlements
            );
        }
    }

    @SuppressForbidden(reason = "Explicitly checking File apis")
    @Override
    public void checkFileWrite(Class<?> callerClass, File file) {
        checkFileWrite(callerClass, file.toPath());
    }

    @Override
    public void checkFileWrite(Class<?> callerClass, Path path) {
        if (PolicyCheckerImpl.isPathOnDefaultFilesystem(path) == false) {
            return;
        }
        var requestingClass = requestingClass(callerClass);
        if (policyManager.isTriviallyAllowed(requestingClass)) {
            return;
        }

        ModuleEntitlements entitlements = policyManager.getEntitlements(requestingClass);
        if (entitlements.fileAccess().canWrite(path) == false) {
            notEntitled(
                Strings.format(
                    "component [%s], module [%s], class [%s], entitlement [file], operation [write], path [%s]",
                    entitlements.componentName(),
                    PolicyCheckerImpl.getModuleName(requestingClass),
                    requestingClass,
                    path
                ),
                callerClass,
                entitlements
            );
        }
    }

    @SuppressForbidden(reason = "Explicitly checking File apis")
    @Override
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

    @Override
    public void checkCreateTempFile(Class<?> callerClass) {
        // in production there should only ever be a single temp directory
        // so we can safely assume we only need to check the sole element in this stream
        checkFileWrite(callerClass, pathLookup.getBaseDirPaths(TEMP).findFirst().get());
    }

    @Override
    public void checkFileDescriptorRead(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "read file descriptor");
    }

    @Override
    public void checkFileDescriptorWrite(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "write file descriptor");
    }

    /**
     * Invoked when we try to get an arbitrary {@code FileAttributeView} class. Such a class can modify attributes, like owner etc.;
     * we could think about introducing checks for each of the operations, but for now we over-approximate this and simply deny when it is
     * used directly.
     */
    @Override
    public void checkGetFileAttributeView(Class<?> callerClass) {
        neverEntitled(callerClass, () -> "get file attribute view");
    }

    /**
     * Check for operations that can access sensitive network information, e.g. secrets, tokens or SSL sessions
     */
    @Override
    public void checkLoadingNativeLibraries(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, LoadNativeLibrariesEntitlement.class);
    }

    private String operationDescription(String methodName) {
        // TODO: Use a more human-readable description. Perhaps share code with InstrumentationServiceImpl.parseCheckerMethodName
        return methodName.substring(methodName.indexOf('$'));
    }

    @Override
    public void checkInboundNetworkAccess(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, InboundNetworkEntitlement.class);
    }

    @Override
    public void checkOutboundNetworkAccess(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, OutboundNetworkEntitlement.class);
    }

    @Override
    public void checkAllNetworkAccess(Class<?> callerClass) {
        var requestingClass = requestingClass(callerClass);
        if (policyManager.isTriviallyAllowed(requestingClass)) {
            return;
        }

        var classEntitlements = policyManager.getEntitlements(requestingClass);
        checkFlagEntitlement(classEntitlements, InboundNetworkEntitlement.class, requestingClass, callerClass);
        checkFlagEntitlement(classEntitlements, OutboundNetworkEntitlement.class, requestingClass, callerClass);
    }

    @Override
    public void checkUnsupportedURLProtocolConnection(Class<?> callerClass, String protocol) {
        neverEntitled(callerClass, () -> Strings.format("unsupported URL protocol [%s]", protocol));
    }

    @Override
    public void checkWriteProperty(Class<?> callerClass, String property) {
        var requestingClass = requestingClass(callerClass);
        if (policyManager.isTriviallyAllowed(requestingClass)) {
            return;
        }

        ModuleEntitlements entitlements = policyManager.getEntitlements(requestingClass);
        if (entitlements.getEntitlements(WriteSystemPropertiesEntitlement.class).anyMatch(e -> e.properties().contains(property))) {
            entitlements.logger()
                .debug(
                    () -> Strings.format(
                        "Entitled: component [%s], module [%s], class [%s], entitlement [write_system_properties], property [%s]",
                        entitlements.componentName(),
                        PolicyCheckerImpl.getModuleName(requestingClass),
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
                PolicyCheckerImpl.getModuleName(requestingClass),
                requestingClass,
                property
            ),
            callerClass,
            entitlements
        );
    }

    @Override
    public void checkManageThreadsEntitlement(Class<?> callerClass) {
        checkEntitlementPresent(callerClass, ManageThreadsEntitlement.class);
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
            .walk(frames -> findRequestingFrame(frames).map(StackWalker.StackFrame::getDeclaringClass));
        return result.orElse(null);
    }

    /**
     * Given a stream of {@link StackWalker.StackFrame}s, identify the one whose entitlements should be checked.
     */
    Optional<StackWalker.StackFrame> findRequestingFrame(Stream<StackWalker.StackFrame> frames) {
        return frames.filter(f -> f.getDeclaringClass().getModule() != entitlementsModule) // ignore entitlements library
            .skip(1) // Skip the sensitive caller method
            .findFirst();
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
                    PolicyCheckerImpl.getModuleName(requestingClass),
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
                    PolicyCheckerImpl.getModuleName(requestingClass),
                    requestingClass,
                    PolicyParser.buildEntitlementNameFromClass(entitlementClass)
                )
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

    @Override
    public void checkEntitlementPresent(Class<?> callerClass, Class<? extends Entitlement> entitlementClass) {
        var requestingClass = requestingClass(callerClass);
        if (policyManager.isTriviallyAllowed(requestingClass)) {
            return;
        }
        checkFlagEntitlement(policyManager.getEntitlements(requestingClass), entitlementClass, requestingClass, callerClass);
    }

}
