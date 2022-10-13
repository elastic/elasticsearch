/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.core.SuppressForbidden;

import java.io.FilePermission;
import java.io.IOException;
import java.net.URLClassLoader;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.DomainCombiner;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.function.Supplier;

import javax.tools.JavaCompiler;

/**
 * A small set of privileged operations that can be executed by unprivileged test code.
 * The set of operations is deliberately small, and the permissions narrow.
 */
public final class PrivilegedOperations {

    private PrivilegedOperations() {}

    public static void closeURLClassLoader(URLClassLoader loader) throws IOException {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                loader.close();
                return null;
            }, context, new RuntimePermission("closeClassLoader"));
        } catch (PrivilegedActionException pae) {
            Exception e = pae.getException();
            if (e instanceof IOException ioe) {
                throw ioe;
            } else {
                throw new IOException(e);
            }
        }
    }

    public static Boolean compilationTaskCall(JavaCompiler.CompilationTask compilationTask) {
        return AccessController.doPrivileged(
            (PrivilegedAction<Boolean>) () -> compilationTask.call(),
            context,
            new RuntimePermission("createClassLoader"),
            new RuntimePermission("closeClassLoader"),
            new RuntimePermission("accessSystemModules"),
            newAllFilesReadPermission()
        );
    }

    public static <T> T supplierWithCreateClassLoader(Supplier<T> supplier) {
        return AccessController.doPrivileged(
            (PrivilegedAction<T>) () -> supplier.get(),
            context,
            new RuntimePermission("createClassLoader"),
            new RuntimePermission("closeClassLoader")
        );
    }

    @SuppressForbidden(reason = "need to create file permission")
    private static FilePermission newAllFilesReadPermission() {
        return new FilePermission("<<ALL FILES>>", "read");
    }

    // -- security manager related stuff, to facilitate asserting permissions for test operations.

    @SuppressWarnings("removal")
    private static AccessControlContext getContext() {
        ProtectionDomain[] pda = new ProtectionDomain[] { privilegedCall(org.elasticsearch.secure_sm.SecureSM.class::getProtectionDomain) };
        DomainCombiner combiner = (ignoreCurrent, ignoreAssigned) -> pda;
        AccessControlContext acc = new AccessControlContext(AccessController.getContext(), combiner);
        // getContext must be called with the new acc so that a combined context will be created
        return AccessController.doPrivileged((PrivilegedAction<AccessControlContext>) AccessController::getContext, acc);
    }

    // Use the all-powerful protection domain of secure_sm for wrapping calls
    @SuppressWarnings("removal")
    private static final AccessControlContext context = getContext();

    @SuppressWarnings("removal")
    private static <T> T privilegedCall(Supplier<T> supplier) {
        return AccessController.doPrivileged((PrivilegedAction<T>) supplier::get, context);
    }
}
