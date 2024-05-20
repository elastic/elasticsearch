/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.SystemdLibrary;

class LinuxNativeAccess extends PosixNativeAccess {

    Systemd systemd;

    LinuxNativeAccess(NativeLibraryProvider libraryProvider) {
        super("Linux", libraryProvider, new PosixConstants(-1L, 9, 1));
        this.systemd = new Systemd(libraryProvider.getLibrary(SystemdLibrary.class));
    }

    @Override
    protected long getMaxThreads() {
        // this is only valid on Linux and the value *is* different on OS X
        // see /usr/include/sys/resource.h on OS X
        // on Linux the resource RLIMIT_NPROC means *the number of threads*
        // this is in opposition to BSD-derived OSes
        final int rlimit_nproc = 6;
        return getRLimit(rlimit_nproc, "max number of threads");
    }

    @Override
    public Systemd systemd() {
        return systemd;
    }
}
