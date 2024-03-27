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
import java.util.Optional;

abstract class PosixNativeAccess extends AbstractNativeAccess {

    protected final PosixCLibrary libc;
    protected final VectorSimilarityFunctions vectorDistance;

    PosixNativeAccess(String name, NativeLibraryProvider libraryProvider) {
        super(name, libraryProvider);
        this.libc = libraryProvider.getLibrary(PosixCLibrary.class);
        this.vectorDistance = Runtime.version().feature() >= 21
            ? new VectorSimilarityFunctions(libraryProvider.getLibrary(VectorLibrary.class))
            : null;
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        return libc.geteuid() == 0;
    }

    @Override
    public Optional<VectorSimilarityFunctions> getVectorSimilarityFunctions() {
        return Optional.ofNullable(vectorDistance);
    }

    // static boolean isNativeVectorLibSupported() { TODO: remove
    // return Runtime.version().feature() >= 21 && isMacOrLinuxAarch64() && checkEnableSystemProperty();
    // }

    /** Returns true iff the OS is Mac or Linux, and the architecture is aarch64. */
    static boolean isMacOrLinuxAarch64() {
        String name = getProperty("os.name");
        return (name.startsWith("Mac") || name.startsWith("Linux")) && getProperty("os.arch").equals("aarch64");
    }

    /** -Dorg.elasticsearch.nativeaccess.JdkVectorLibrary=false} to disable.*/
    static final String ENABLE_JDK_VECTOR_LIBRARY = "org.elasticsearch.nativeaccess.JdkVectorLibrary";

    static boolean checkEnableSystemProperty() {
        return Optional.ofNullable(getProperty(ENABLE_JDK_VECTOR_LIBRARY)).map(Boolean::valueOf).orElse(Boolean.TRUE);
    }

    @SuppressWarnings("removal")
    static String getProperty(String name) {
        PrivilegedAction<String> pa = () -> System.getProperty(name);
        return AccessController.doPrivileged(pa);
    }
}
