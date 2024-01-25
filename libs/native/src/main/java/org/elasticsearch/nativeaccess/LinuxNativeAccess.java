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
import org.elasticsearch.nativeaccess.lib.PosixCLibrary.RLimit;

class LinuxNativeAccess extends PosixNativeAccess {

    // set to the maximum number of threads that can be created for
    // the user ID that owns the running Elasticsearch process
    private long maxNumberOfThreads = -1;

    LinuxNativeAccess(NativeLibraryProvider libraryProvider) {
        super(libraryProvider, 8, -1L, 9);
    }

    @Override
    protected void logMemoryLimitInstructions() {
        // give specific instructions for the linux case to make it easy
        String user = System.getProperty("user.name");
        logger.warn("""
                        These can be adjusted by modifying /etc/security/limits.conf, for example:
                        \t# allow user '{}' mlockall
                        \t{} soft memlock unlimited
                        \t{} hard memlock unlimited""", user, user, user);
        logger.warn("If you are logged in interactively, you will have to re-login for the new limits to take effect.");
    }

    @Override
    public void trySetMaxNumberOfThreads() {
        // this is only valid on Linux and the value *is* different on OS X
        // see /usr/include/sys/resource.h on OS X
        // on Linux the resource RLIMIT_NPROC means *the number of threads*
        // this is in opposition to BSD-derived OSes
        final int rlimit_nproc = 6;

        var rlimit = new RLimit();
        if (libc.getrlimit(rlimit_nproc, rlimit) == 0) {
            maxNumberOfThreads = rlimit.rlim_cur;
        } else {
            logger.warn("unable to retrieve max number of threads [" + libc.strerror(libc.errno()) + "]");
        }
    }
}
