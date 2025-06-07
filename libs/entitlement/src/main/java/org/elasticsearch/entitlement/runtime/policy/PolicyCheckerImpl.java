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
import java.net.HttpURLConnection;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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
@SuppressForbidden(reason = "Explicitly checking APIs that are forbidden")
public class PolicyCheckerImpl implements PolicyChecker {
    static final Class<?> DEFAULT_FILESYSTEM_CLASS = PathUtils.getDefaultFileSystem().getClass();
    protected final Set<Package> suppressFailureLogPackages;
    /**
     * Frames originating from this module are ignored in the permission logic.
     */
    protected final Module entitlementsModule;

    private final PolicyManager policyManager;

    private final PathLookup pathLookup;

    public PolicyCheckerImpl(
        Set<Package> suppressFailureLogPackages,
        Module entitlementsModule,
        PolicyManager policyManager,
        PathLookup pathLookup
    ) {
        this.suppressFailureLogPackages = suppressFailureLogPackages;
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
        // Don't emit a log for suppressed packages, e.g. packages containing self tests
        if (suppressFailureLogPackages.contains(callerClass.getPackage()) == false) {
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
        ModuleEntitlements entitlements = policyManager.getEntitlements(requestingClass);
        checkFlagEntitlement(entitlements, entitlementClass, requestingClass, callerClass);
    }

    @Override
    public void checkEntitlementForUrl(Class<?> callerClass, URL url) {
        if (handleNetworkOrFileUrlCheck(callerClass, url)) {
            return;
        }
        if (isJarUrl(url)) {
            var jarFileUrl = extractJarFileUrl(url);
            if (jarFileUrl == null || handleNetworkOrFileUrlCheck(callerClass, jarFileUrl) == false) {
                checkUnsupportedURLProtocolConnection(callerClass, "jar with unsupported inner protocol");
            }
        } else {
            checkUnsupportedURLProtocolConnection(callerClass, url.getProtocol());
        }
    }

    @Override
    public void checkEntitlementForURLConnection(Class<?> callerClass, URLConnection urlConnection) {
        if (isNetworkUrlConnection(urlConnection)) {
            checkOutboundNetworkAccess(callerClass);
        } else if (isFileUrlConnection(urlConnection)) {
            checkURLFileRead(callerClass, urlConnection.getURL());
        } else if (urlConnection instanceof JarURLConnection jarURLConnection) {
            checkJarURLAccess(callerClass, jarURLConnection);
        } else {
            checkUnsupportedURLProtocolConnection(callerClass, urlConnection.getURL().getProtocol());
        }
    }

    @SuppressWarnings("deprecation")
    private URL extractJarFileUrl(URL jarUrl) {
        String spec = jarUrl.getFile();
        int separator = spec.indexOf("!/");

        // URL does not handle nested JAR URLs (it would be a MalformedURLException upon connection)
        if (separator == -1) {
            return null;
        }

        try {
            return new URL(spec.substring(0, separator));
        } catch (MalformedURLException e) {
            return null;
        }
    }

    private boolean handleNetworkOrFileUrlCheck(Class<?> callerClass, URL url) {
        if (isNetworkUrl(url)) {
            checkOutboundNetworkAccess(callerClass);
            return true;
        }
        if (isFileUrl(url)) {
            checkURLFileRead(callerClass, url);
            return true;
        }
        return false;
    }

    @Override
    public void checkJarURLAccess(Class<?> callerClass, JarURLConnection connection) {
        var jarFileUrl = connection.getJarFileURL();
        if (handleNetworkOrFileUrlCheck(callerClass, jarFileUrl)) {
            return;
        }
        checkUnsupportedURLProtocolConnection(callerClass, jarFileUrl.getProtocol());
    }

    private static final Set<String> NETWORK_PROTOCOLS = Set.of("http", "https", "ftp", "mailto");

    private static boolean isNetworkUrl(java.net.URL url) {
        return NETWORK_PROTOCOLS.contains(url.getProtocol());
    }

    private static boolean isFileUrl(java.net.URL url) {
        return "file".equals(url.getProtocol());
    }

    private static boolean isJarUrl(java.net.URL url) {
        return "jar".equals(url.getProtocol());
    }

    // We have to use class names for sun.net.www classes as java.base does not export them
    private static final List<String> ADDITIONAL_NETWORK_URL_CONNECT_CLASS_NAMES = List.of(
        "sun.net.www.protocol.ftp.FtpURLConnection",
        "sun.net.www.protocol.mailto.MailToURLConnection"
    );

    private static boolean isNetworkUrlConnection(java.net.URLConnection urlConnection) {
        var connectionClass = urlConnection.getClass();
        return HttpURLConnection.class.isAssignableFrom(connectionClass)
            || ADDITIONAL_NETWORK_URL_CONNECT_CLASS_NAMES.contains(connectionClass.getName());
    }

    // We have to use class names for sun.net.www classes as java.base does not export them
    private static boolean isFileUrlConnection(java.net.URLConnection urlConnection) {
        var connectionClass = urlConnection.getClass();
        return "sun.net.www.protocol.file.FileURLConnection".equals(connectionClass.getName());
    }

    @Override
    public void checkURLFileRead(Class<?> callerClass, URL url) {
        try {
            checkFileRead(callerClass, Paths.get(url.toURI()));
        } catch (URISyntaxException e) {
            // We expect this method to be called only on File URLs; otherwise the underlying method would fail anyway
            throw new RuntimeException(e);
        }
    }

}
