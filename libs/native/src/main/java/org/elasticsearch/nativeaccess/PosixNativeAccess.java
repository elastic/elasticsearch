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

abstract class PosixNativeAccess extends AbstractNativeAccess {

    protected final PosixCLibrary libc;

    PosixNativeAccess(String name, NativeLibraryProvider libraryProvider) {
        super(name, libraryProvider);
        this.libc = libraryProvider.getLibrary(PosixCLibrary.class);
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        return libc.geteuid() == 0;
    }
}
