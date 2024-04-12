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

import java.util.Optional;

abstract class PosixNativeAccess extends AbstractNativeAccess {

    protected final PosixCLibrary libc;
    protected final VectorSimilarityFunctions vectorDistance;

    PosixNativeAccess(String name, NativeLibraryProvider libraryProvider) {
        super(name, libraryProvider);
        this.libc = libraryProvider.getLibrary(PosixCLibrary.class);
        this.vectorDistance = vectorSimilarityFunctionsOrNull(libraryProvider);
    }

    static VectorSimilarityFunctions vectorSimilarityFunctionsOrNull(NativeLibraryProvider libraryProvider) {
        if (isNativeVectorLibSupported()) {
            var lib = libraryProvider.getLibrary(VectorLibrary.class).getVectorSimilarityFunctions();
            logger.info("Using native vector library; to disable start with -D" + ENABLE_JDK_VECTOR_LIBRARY + "=false");
            return lib;
        }
        return null;
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        return libc.geteuid() == 0;
    }

    @Override
    public Optional<VectorSimilarityFunctions> getVectorSimilarityFunctions() {
        return Optional.ofNullable(vectorDistance);
    }

    static boolean isNativeVectorLibSupported() {
        return Runtime.version().feature() >= 21 && isMacOrLinuxAarch64() && checkEnableSystemProperty();
    }

    /** Returns true iff the OS is Mac or Linux, and the architecture is aarch64. */
    static boolean isMacOrLinuxAarch64() {
        String name = System.getProperty("os.name");
        return (name.startsWith("Mac") || name.startsWith("Linux")) && System.getProperty("os.arch").equals("aarch64");
    }

    /** -Dorg.elasticsearch.nativeaccess.enableVectorLibrary=false to disable.*/
    static final String ENABLE_JDK_VECTOR_LIBRARY = "org.elasticsearch.nativeaccess.enableVectorLibrary";

    static boolean checkEnableSystemProperty() {
        return Optional.ofNullable(System.getProperty(ENABLE_JDK_VECTOR_LIBRARY)).map(Boolean::valueOf).orElse(Boolean.TRUE);
    }
}
