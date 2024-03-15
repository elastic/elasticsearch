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

    private static boolean isNativeVectorLibSupported() {
        return Runtime.version().feature() >= 21 && System.getProperty("os.arch").equals("aarch64");
    }
}
