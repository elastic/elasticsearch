/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.rules.function.Call1;
import org.elasticsearch.entitlement.rules.function.Call2;
import org.elasticsearch.entitlement.rules.function.CheckMethod;

import java.io.File;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

/**
 * Factory class providing predefined entitlement policy check methods.
 * <p>
 * This class contains static factory methods that create {@link CheckMethod} instances
 * for various system operations that require entitlement checks. Each method returns
 * a {@link CheckMethod} that can be used in entitlement rule definitions to enforce
 * security policies on sensitive operations.
 */
public class Policies {

    /**
     * Returns a no-op check method that performs no entitlement checks.
     *
     * @return a check method that does nothing
     */
    public static CheckMethod empty() {
        return (callingClass, policyChecker) -> {};
    }

    /**
     * Returns a check method for all network access operations.
     *
     * @return a check method that verifies entitlement for all network access
     */
    public static CheckMethod allNetworkAccess() {
        return (callingClass, policyChecker) -> policyChecker.checkAllNetworkAccess(callingClass);
    }

    /**
     * Returns a check method for operations that change file handling behavior.
     *
     * @return a check method that verifies entitlement for changing file handling
     */
    public static CheckMethod changeFilesHandling() {
        return (callingClass, policyChecker) -> policyChecker.checkChangeFilesHandling(callingClass);
    }

    /**
     * Returns a check method for operations that change JVM global state.
     *
     * @return a check method that verifies entitlement for changing JVM global state
     */
    public static CheckMethod changeJvmGlobalState() {
        return (callingClass, policyChecker) -> policyChecker.checkChangeJVMGlobalState(callingClass);
    }

    /**
     * Returns a check method for operations that change network handling behavior.
     *
     * @return a check method that verifies entitlement for changing network handling
     */
    public static CheckMethod changeNetworkHandling() {
        return (callingClass, policyChecker) -> policyChecker.checkChangeNetworkHandling(callingClass);
    }

    /**
     * Returns a check method for creating class loaders.
     *
     * @return a check method that verifies entitlement for creating class loaders
     */
    public static CheckMethod createClassLoader() {
        return (callingClass, policyChecker) -> policyChecker.checkCreateClassLoader(callingClass);
    }

    /**
     * Returns a check method for creating temporary files.
     *
     * @return a check method that verifies entitlement for creating temporary files
     */
    public static CheckMethod createTempFile() {
        return (callingClass, policyChecker) -> policyChecker.checkCreateTempFile(callingClass);
    }

    /**
     * Returns a check method for URL access.
     *
     * @param url the URL being accessed
     * @return a check method that verifies entitlement for the specified URL
     */
    public static CheckMethod entitlementForUrl(URL url) {
        return (callingClass, policyChecker) -> policyChecker.checkEntitlementForUrl(callingClass, url);
    }

    /**
     * Returns a check method for URL connection access.
     *
     * @param urlConnection the URL connection being accessed
     * @return a check method that verifies entitlement for the specified URL connection
     */
    public static CheckMethod entitlementForUrlConnection(URLConnection urlConnection) {
        return (callingClass, policyChecker) -> policyChecker.checkEntitlementForURLConnection(callingClass, urlConnection);
    }

    /**
     * Returns a check method for VM exit operations.
     *
     * @return a check method that verifies entitlement for exiting the VM
     */
    public static CheckMethod exitVM() {
        return (callingClass, policyChecker) -> policyChecker.checkExitVM(callingClass);
    }

    /**
     * Returns a check method for reading from file descriptors.
     *
     * @return a check method that verifies entitlement for file descriptor reads
     */
    public static CheckMethod fileDescriptorRead() {
        return (callingClass, policyChecker) -> policyChecker.checkFileDescriptorRead(callingClass);
    }

    /**
     * Returns a check method for writing to file descriptors.
     *
     * @return a check method that verifies entitlement for file descriptor writes
     */
    public static CheckMethod fileDescriptorWrite() {
        return (callingClass, policyChecker) -> policyChecker.checkFileDescriptorWrite(callingClass);
    }

    /**
     * Returns a check method for reading a file.
     *
     * @param file the file being read
     * @return a check method that verifies entitlement for reading the specified file
     */
    @SuppressForbidden(reason = "Instrumenting JDK File-based APIs")
    public static CheckMethod fileRead(File file) {
        return (callingClass, policyChecker) -> policyChecker.checkFileRead(callingClass, file);
    }

    /**
     * Returns a check method for reading a file.
     *
     * @param path the path being read
     * @return a check method that verifies entitlement for reading the specified path
     */
    public static CheckMethod fileRead(Path path) {
        return (callingClass, policyChecker) -> policyChecker.checkFileRead(callingClass, path);
    }

    /**
     * Returns a check method for reading a file with link handling options.
     *
     * @param path the path being read
     * @param followLinks whether symbolic links should be followed
     * @return a check method that verifies entitlement for reading the specified path
     */
    public static CheckMethod fileReadWithLinks(Path path, boolean followLinks) {
        return (callingClass, policyChecker) -> policyChecker.checkFileRead(callingClass, path, followLinks);
    }

    /**
     * Returns a check method for creating file links.
     * <p>
     * This method checks both write entitlement for the link location and read
     * entitlement for the target location.
     *
     * @param link the link path being created
     * @param target the target path the link points to
     * @return a check method that verifies entitlements for creating the specified link
     */
    public static CheckMethod createLink(Path link, Path target) {
        return (callingClass, policyChecker) -> {
            policyChecker.checkFileWrite(callingClass, link);
            policyChecker.checkFileRead(callingClass, resolveLinkTarget(link, target));
        };
    }

    /**
     * Returns a check method for accessing a file with specific ZIP mode.
     *
     * @param file the file being accessed
     * @param mode the ZIP mode
     * @return a check method that verifies entitlement for the specified file and mode
     */
    @SuppressForbidden(reason = "Instrumenting JDK File-based APIs")
    public static CheckMethod fileWithZipMode(File file, int mode) {
        return (callingClass, policyChecker) -> policyChecker.checkFileWithZipMode(callingClass, file, mode);
    }

    /**
     * Returns a check method for writing to a file.
     *
     * @param file the file being written to
     * @return a check method that verifies entitlement for writing to the specified file
     */
    @SuppressForbidden(reason = "Instrumenting JDK File-based APIs")
    public static CheckMethod fileWrite(File file) {
        return (callingClass, policyChecker) -> policyChecker.checkFileWrite(callingClass, file);
    }

    /**
     * Returns a check method for writing to a file.
     *
     * @param path the path being written to
     * @return a check method that verifies entitlement for writing to the specified path
     */
    public static CheckMethod fileWrite(Path path) {
        return (callingClass, policyChecker) -> policyChecker.checkFileWrite(callingClass, path);
    }

    /**
     * Returns a check method for getting file attribute views.
     *
     * @return a check method that verifies entitlement for getting file attribute views
     */
    public static CheckMethod getFileAttributeView() {
        return (callingClass, policyChecker) -> policyChecker.checkGetFileAttributeView(callingClass);
    }

    /**
     * Returns a check method for inbound network access operations.
     *
     * @return a check method that verifies entitlement for inbound network access
     */
    public static CheckMethod inboundNetworkAccess() {
        return (callingClass, policyChecker) -> policyChecker.checkInboundNetworkAccess(callingClass);
    }

    /**
     * Returns a check method for accessing JAR URLs.
     *
     * @param jarURLConnection the JAR URL connection being accessed
     * @return a check method that verifies entitlement for the specified JAR URL connection
     */
    public static CheckMethod jarURLAccess(JarURLConnection jarURLConnection) {
        return (callingClass, policyChecker) -> policyChecker.checkJarURLAccess(callingClass, jarURLConnection);
    }

    /**
     * Returns a check method for loading native libraries.
     *
     * @return a check method that verifies entitlement for loading native libraries
     */
    public static CheckMethod loadingNativeLibraries() {
        return (callingClass, policyChecker) -> policyChecker.checkLoadingNativeLibraries(callingClass);
    }

    /**
     * Returns a check method for using logging file handlers.
     *
     * @return a check method that verifies entitlement for logging file handlers
     */
    public static CheckMethod loggingFileHandler() {
        return (callingClass, policyChecker) -> policyChecker.checkLoggingFileHandler(callingClass);
    }

    /**
     * Returns a check method for thread management operations.
     *
     * @return a check method that verifies entitlement for managing threads
     */
    public static CheckMethod manageThreads() {
        return (callingClass, policyChecker) -> policyChecker.checkManageThreadsEntitlement(callingClass);
    }

    /**
     * Returns a check method for outbound network access operations.
     *
     * @return a check method that verifies entitlement for outbound network access
     */
    public static CheckMethod outboundNetworkAccess() {
        return (callingClass, policyChecker) -> policyChecker.checkOutboundNetworkAccess(callingClass);
    }

    /**
     * Returns a check method for reading file store attributes.
     *
     * @return a check method that verifies entitlement for reading store attributes
     */
    public static CheckMethod readStoreAttributes() {
        return (callingClass, policyChecker) -> policyChecker.checkReadStoreAttributes(callingClass);
    }

    /**
     * Returns a check method for setting HTTPS connection properties.
     *
     * @return a check method that verifies entitlement for setting HTTPS connection properties
     */
    public static CheckMethod setHttpsConnectionProperties() {
        return (callingClass, policyChecker) -> policyChecker.checkSetHttpsConnectionProperties(callingClass);
    }

    /**
     * Returns a check method for starting processes.
     *
     * @return a check method that verifies entitlement for starting processes
     */
    public static CheckMethod startProcess() {
        return (callingClass, policyChecker) -> policyChecker.checkStartProcess(callingClass);
    }

    /**
     * Returns a check method for reading files via URL.
     *
     * @param url the URL being used to read a file
     * @return a check method that verifies entitlement for reading via the specified URL
     */
    public static CheckMethod urlFileRead(URL url) {
        return (callingClass, policyChecker) -> policyChecker.checkURLFileRead(callingClass, url);
    }

    /**
     * Returns a check method for writing system properties.
     *
     * @param property the property name being written
     * @return a check method that verifies entitlement for writing the specified property
     */
    public static CheckMethod writeProperty(String property) {
        return (callingClass, policyChecker) -> policyChecker.checkWriteProperty(callingClass, property);
    }

    /**
     * Returns a check method for writing file store attributes.
     *
     * @return a check method that verifies entitlement for writing store attributes
     */
    public static CheckMethod writeStoreAttributes() {
        return (callingClass, policyChecker) -> policyChecker.checkWriteStoreAttributes(callingClass);
    }

    /**
     * Returns a check method for file operations that dynamically determines whether
     * to check read or write entitlements based on the provided options.
     *
     * @param path the path being accessed
     * @param options the set of open options
     * @return a check method that verifies appropriate entitlement based on the options
     */
    public static CheckMethod fileReadOrWrite(Path path, Set<? extends OpenOption> options) {
        return (callingClass, policyChecker) -> {
            if (isOpenForWrite(options)) {
                policyChecker.checkFileWrite(callingClass, path);
            } else {
                policyChecker.checkFileRead(callingClass, path);
            }
        };
    }

    /**
     * Combines two check method suppliers into a single composite check method supplier
     * that executes both checks in sequence.
     *
     * @param <A> the type of the first parameter
     * @param <B> the type of the second parameter
     * @param first the first check method supplier
     * @param second the second check method supplier
     * @return a composite check method supplier that applies both checks
     */
    public static <A, B> Call2<CheckMethod, A, B> and(Call1<CheckMethod, A> first, Call1<CheckMethod, B> second) {
        return (a, b) -> first.call(a).and(second.call(b));
    }

    private static boolean isOpenForWrite(Set<? extends OpenOption> options) {
        return options.contains(StandardOpenOption.WRITE)
            || options.contains(StandardOpenOption.APPEND)
            || options.contains(StandardOpenOption.CREATE)
            || options.contains(StandardOpenOption.CREATE_NEW)
            || options.contains(StandardOpenOption.DELETE_ON_CLOSE);
    }

    private static Path resolveLinkTarget(Path path, Path target) {
        var parent = path.getParent();
        return parent == null ? target : parent.resolve(target);
    }
}
