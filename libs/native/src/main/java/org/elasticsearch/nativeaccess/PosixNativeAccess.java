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

    // libc constants
    protected static final int MCL_CURRENT = 1;
    protected static final int ENOMEM = 12;

    protected final int RLIMIT_MEMLOCK;
    protected final long RLIMIT_INFINITY;
    protected final int RLIMIT_AS;
    protected final int RLIMIT_FSIZE = 1; // same on mac and linux

    protected final PosixCLibrary libc;

    PosixNativeAccess(NativeLibraryProvider libraryProvider, int RLIMIT_MEMLOCK, long RLIMIT_INFINITY, int RLIMIT_AS) {
        this.libc = libraryProvider.getLibrary(PosixCLibrary.class);
        this.RLIMIT_MEMLOCK = RLIMIT_MEMLOCK;
        this.RLIMIT_INFINITY = RLIMIT_INFINITY;
        this.RLIMIT_AS = RLIMIT_AS;
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        return libc.geteuid() == 0;
    }

    @Override
    public void tryLockMemory() {
        int result = libc.mlockall(MCL_CURRENT);
        if (result == 0) {
            memoryLocked = true;
            return;
        }

        // mlockall failed for some reason
        int errno = libc.errno();
        String errMsg = libc.strerror(errno);
        logger.warn("Unable to lock JVM Memory: error={}, reason={}", errno, errMsg);
        logger.warn("This can result in part of the JVM being swapped out.");

        if (errno == ENOMEM) {

            boolean rlimitSuccess = false;
            long softLimit = 0;
            long hardLimit = 0;

            // we only know RLIMIT_MEMLOCK for these two at the moment.
            var rlimit = libc.newRLimit();
            if (libc.getrlimit(RLIMIT_MEMLOCK, rlimit) == 0) {
                rlimitSuccess = true;
                softLimit = rlimit.rlim_cur();
                hardLimit = rlimit.rlim_max();
            } else {
                logger.warn("Unable to retrieve resource limits: {}", libc.strerror(libc.errno()));
            }

            if (rlimitSuccess) {
                logger.warn(
                    "Increase RLIMIT_MEMLOCK, soft limit: {}, hard limit: {}",
                    rlimitToString(softLimit),
                    rlimitToString(hardLimit)
                );
                logMemoryLimitInstructions();
            } else {
                logger.warn("Increase RLIMIT_MEMLOCK (ulimit).");
            }
        }
    }

    protected abstract void logMemoryLimitInstructions();

    @Override
    public void trySetMaxVirtualMemorySize() {
        var rlimit = libc.newRLimit();
        if (libc.getrlimit(RLIMIT_AS, rlimit) == 0) {
            maxVirtualMemorySize = rlimit.rlim_cur();
            System.out.println("retrieved rlimit=" + rlimit);
        } else {
            logger.warn("unable to retrieve max size virtual memory [" + libc.strerror(libc.errno()) + "]");
        }
    }

    @Override
    public void trySetMaxFileSize() {
        var rlimit = libc.newRLimit();
        if (libc.getrlimit(RLIMIT_FSIZE, rlimit) == 0) {
            maxFileSize = rlimit.rlim_cur();
        } else {
            logger.warn("unable to retrieve max file size [" + libc.strerror(libc.errno()) + "]");
        }
    }

    String rlimitToString(long value) {
        if (value == RLIMIT_INFINITY) {
            return "unlimited";
        } else {
            return Long.toUnsignedString(value);
        }
    }
}
