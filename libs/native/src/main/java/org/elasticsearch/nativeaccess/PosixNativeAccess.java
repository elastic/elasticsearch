/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.CLibrary;

abstract class PosixNativeAccess extends NativeAccess {

    // libc constants
    private static final int MCL_CURRENT = 1;
    private static final int ENOMEM = 12;

    private final int RLIMIT_MEMLOCK;
    private final long RLIM_INFINITY;

    private final CLibrary libc;

    private boolean mlockallSucceeded = false;

    PosixNativeAccess(CLibrary libc, int RLIMIT_MEMLOCK, long RLIMIT_INFINITY) {
        this.libc = libc;
        this.RLIMIT_MEMLOCK = RLIMIT_MEMLOCK;
        this.RLIM_INFINITY = RLIMIT_INFINITY;
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        return libc.geteuid() == 0;
    }

    @Override
    public void tryLockMemory() {
        int result = libc.mlockall(MCL_CURRENT);
        if (result == 0) {
            mlockallSucceeded = true;
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
            var rlimit = new CLibrary.RLimit();
            if (libc.getrlimit(RLIMIT_MEMLOCK, rlimit) == 0) {
                rlimitSuccess = true;
                softLimit = rlimit.rlim_cur;
                hardLimit = rlimit.rlim_max;
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

    @Override
    public boolean isMemoryLocked() {
        return mlockallSucceeded;
    }

    protected abstract void logMemoryLimitInstructions();

    String rlimitToString(long value) {
        if (value == RLIM_INFINITY) {
            return "unlimited";
        } else {
            return Long.toUnsignedString(value);
        }
    }
}
