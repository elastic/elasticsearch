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
        super("Linux", libraryProvider);
        this.systemd = new Systemd(libraryProvider.getLibrary(SystemdLibrary.class));
    }

    @Override
    public Systemd systemd() {
        return systemd;
    }
}
