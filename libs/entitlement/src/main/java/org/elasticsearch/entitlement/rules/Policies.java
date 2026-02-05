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

public class Policies {

    public static CheckMethod empty() {
        return (callingClass, policyChecker) -> {};
    }

    public static CheckMethod allNetworkAccess() {
        return (callingClass, policyChecker) -> policyChecker.checkAllNetworkAccess(callingClass);
    }

    public static CheckMethod changeFilesHandling() {
        return (callingClass, policyChecker) -> policyChecker.checkChangeFilesHandling(callingClass);
    }

    public static CheckMethod changeJvmGlobalState() {
        return (callingClass, policyChecker) -> policyChecker.checkChangeJVMGlobalState(callingClass);
    }

    public static CheckMethod changeNetworkHandling() {
        return (callingClass, policyChecker) -> policyChecker.checkChangeNetworkHandling(callingClass);
    }

    public static CheckMethod createClassLoader() {
        return (callingClass, policyChecker) -> policyChecker.checkCreateClassLoader(callingClass);
    }

    public static CheckMethod createTempFile() {
        return (callingClass, policyChecker) -> policyChecker.checkCreateTempFile(callingClass);
    }

    public static CheckMethod entitlementForUrl(URL url) {
        return (callingClass, policyChecker) -> policyChecker.checkEntitlementForUrl(callingClass, url);
    }

    public static CheckMethod entitlementForUrlConnection(URLConnection urlConnection) {
        return (callingClass, policyChecker) -> policyChecker.checkEntitlementForURLConnection(callingClass, urlConnection);
    }

    public static CheckMethod exitVM() {
        return (callingClass, policyChecker) -> policyChecker.checkExitVM(callingClass);
    }

    public static CheckMethod fileDescriptorRead() {
        return (callingClass, policyChecker) -> policyChecker.checkFileDescriptorRead(callingClass);
    }

    public static CheckMethod fileDescriptorWrite() {
        return (callingClass, policyChecker) -> policyChecker.checkFileDescriptorWrite(callingClass);
    }

    @SuppressForbidden(reason = "Instrumenting JDK File-based APIs")
    public static CheckMethod fileRead(File file) {
        return (callingClass, policyChecker) -> policyChecker.checkFileRead(callingClass, file);
    }

    public static CheckMethod fileRead(Path path) {
        return (callingClass, policyChecker) -> policyChecker.checkFileRead(callingClass, path);
    }

    public static CheckMethod fileReadWithLinks(Path path, boolean followLinks) {
        return (callingClass, policyChecker) -> policyChecker.checkFileRead(callingClass, path, followLinks);
    }

    public static CheckMethod createLink(Path link, Path target) {
        return (callingClass, policyChecker) -> {
            policyChecker.checkFileWrite(callingClass, link);
            policyChecker.checkFileRead(callingClass, resolveLinkTarget(link, target));
        };
    }

    @SuppressForbidden(reason = "Instrumenting JDK File-based APIs")
    public static CheckMethod fileWithZipMode(File file, int mode) {
        return (callingClass, policyChecker) -> policyChecker.checkFileWithZipMode(callingClass, file, mode);
    }

    @SuppressForbidden(reason = "Instrumenting JDK File-based APIs")
    public static CheckMethod fileWrite(File file) {
        return (callingClass, policyChecker) -> policyChecker.checkFileWrite(callingClass, file);
    }

    public static CheckMethod fileWrite(Path path) {
        return (callingClass, policyChecker) -> policyChecker.checkFileWrite(callingClass, path);
    }

    public static CheckMethod getFileAttributeView() {
        return (callingClass, policyChecker) -> policyChecker.checkGetFileAttributeView(callingClass);
    }

    public static CheckMethod inboundNetworkAccess() {
        return (callingClass, policyChecker) -> policyChecker.checkInboundNetworkAccess(callingClass);
    }

    public static CheckMethod jarURLAccess(JarURLConnection jarURLConnection) {
        return (callingClass, policyChecker) -> policyChecker.checkJarURLAccess(callingClass, jarURLConnection);
    }

    public static CheckMethod loadingNativeLibraries() {
        return (callingClass, policyChecker) -> policyChecker.checkLoadingNativeLibraries(callingClass);
    }

    public static CheckMethod loggingFileHandler() {
        return (callingClass, policyChecker) -> policyChecker.checkLoggingFileHandler(callingClass);
    }

    public static CheckMethod manageThreads() {
        return (callingClass, policyChecker) -> policyChecker.checkManageThreadsEntitlement(callingClass);
    }

    public static CheckMethod outboundNetworkAccess() {
        return (callingClass, policyChecker) -> policyChecker.checkOutboundNetworkAccess(callingClass);
    }

    public static CheckMethod readStoreAttributes() {
        return (callingClass, policyChecker) -> policyChecker.checkReadStoreAttributes(callingClass);
    }

    public static CheckMethod setHttpsConnectionProperties() {
        return (callingClass, policyChecker) -> policyChecker.checkSetHttpsConnectionProperties(callingClass);
    }

    public static CheckMethod startProcess() {
        return (callingClass, policyChecker) -> policyChecker.checkStartProcess(callingClass);
    }

    public static CheckMethod urlFileRead(URL url) {
        return (callingClass, policyChecker) -> policyChecker.checkURLFileRead(callingClass, url);
    }

    public static CheckMethod writeProperty(String property) {
        return (callingClass, policyChecker) -> policyChecker.checkWriteProperty(callingClass, property);
    }

    public static CheckMethod writeStoreAttributes() {
        return (callingClass, policyChecker) -> policyChecker.checkWriteStoreAttributes(callingClass);
    }

    public static CheckMethod fileReadOrWrite(Path path, Set<? extends OpenOption> options) {
        return (callingClass, policyChecker) -> {
            if (isOpenForWrite(options)) {
                policyChecker.checkFileWrite(callingClass, path);
            } else {
                policyChecker.checkFileRead(callingClass, path);
            }
        };
    }

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
