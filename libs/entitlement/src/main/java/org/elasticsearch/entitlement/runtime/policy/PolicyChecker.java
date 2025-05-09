/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;

import java.io.File;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

/**
 * Contains one "check" method for each distinct kind of check we do
 * (as opposed to {@link org.elasticsearch.entitlement.bridge.EntitlementChecker},
 * which has a method for each distinct <em>>method</em> we instrument).
 */
@SuppressForbidden(reason = "Explicitly checking APIs that are forbidden")
public interface PolicyChecker {
    void checkAllNetworkAccess(Class<?> callerClass);

    void checkChangeFilesHandling(Class<?> callerClass);

    void checkChangeJVMGlobalState(Class<?> callerClass);

    void checkChangeNetworkHandling(Class<?> callerClass);

    void checkCreateClassLoader(Class<?> callerClass);

    void checkCreateTempFile(Class<?> callerClass);

    void checkEntitlementPresent(Class<?> callerClass, Class<? extends Entitlement> entitlementClass);

    void checkEntitlementForUrl(Class<?> callerClass, URL url);

    void checkEntitlementForURLConnection(Class<?> callerClass, URLConnection urlConnection);

    void checkExitVM(Class<?> callerClass);

    void checkFileDescriptorRead(Class<?> callerClass);

    void checkFileDescriptorWrite(Class<?> callerClass);

    void checkFileRead(Class<?> callerClass, File file);

    void checkFileRead(Class<?> callerClass, Path path, boolean followLinks) throws NoSuchFileException;

    void checkFileRead(Class<?> callerClass, Path path);

    void checkFileWithZipMode(Class<?> callerClass, File file, int zipMode);

    void checkFileWrite(Class<?> callerClass, File file);

    void checkFileWrite(Class<?> callerClass, Path path);

    void checkGetFileAttributeView(Class<?> callerClass);

    void checkInboundNetworkAccess(Class<?> callerClass);

    void checkJarURLAccess(Class<?> callerClass, JarURLConnection connection);

    void checkLoadingNativeLibraries(Class<?> callerClass);

    void checkLoggingFileHandler(Class<?> callerClass);

    void checkManageThreadsEntitlement(Class<?> callerClass);

    void checkOutboundNetworkAccess(Class<?> callerClass);

    void checkReadStoreAttributes(Class<?> callerClass);

    void checkSetHttpsConnectionProperties(Class<?> callerClass);

    void checkStartProcess(Class<?> callerClass);

    void checkUnsupportedURLProtocolConnection(Class<?> callerClass, String protocol);

    void checkURLFileRead(Class<?> callerClass, URL url);

    void checkWriteProperty(Class<?> callerClass, String property);

    void checkWriteStoreAttributes(Class<?> callerClass);
}
