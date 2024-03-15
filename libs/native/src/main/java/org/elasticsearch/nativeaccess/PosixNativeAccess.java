/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;

import java.security.AccessController;
import java.security.PrivilegedAction;

abstract class PosixNativeAccess extends AbstractNativeAccess {

    protected final PosixCLibrary libc;
    protected final VectorScorerFactory vectorScorerFactory;

    PosixNativeAccess(String name, NativeLibraryProvider libraryProvider) {
        super(name, libraryProvider);
        this.libc = libraryProvider.getLibrary(PosixCLibrary.class);
        this.vectorScorerFactory = isNativeVectorLibSupported() ? libraryProvider.getLibrary(VectorLibrary.class) : null;
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        return libc.geteuid() == 0;
    }

    @Override
    public VectorScorerFactory getVectorScorerFactory() {
        return vectorScorerFactory;
    }

    static boolean isNativeVectorLibSupported() {
        return Runtime.version().feature() >= 21 && getProperty("os.arch").equals("aarch64") && isMacOrLinux();
    }

    static boolean isMacOrLinux() {
        String name = getProperty("os.name");
        return name.startsWith("Mac") || name.startsWith("Linux");
    }

    @SuppressWarnings("removal")
    static String getProperty(String name) {
        PrivilegedAction<String> pa = () -> System.getProperty(name);
        return AccessController.doPrivileged(pa);
    }
}
