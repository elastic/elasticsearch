/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

/**
 * Provides access to methods in libc.so available on POSIX systems.
 */
public non-sealed interface PosixCLibrary extends NativeLibrary {

    /**
     * Gets the effective userid of the current process.
     *
     * @return the effective user id
     * @see <a href="https://pubs.opengroup.org/onlinepubs/9699919799/functions/geteuid.html">geteuid</a>
     */
    int geteuid();

    int mlockall(int flags);

    /** corresponds to struct rlimit */
    interface RLimit {
        long rlim_cur();

        long rlim_max();

        void rlim_cur(long v);

        void rlim_max(long v);
    }

    RLimit newRLimit();

    /** corresponds to struct stat64 */
    interface Stat64 {
        long st_size();
    }

    Stat64 newStat64(int sizeof, int stSizeOffset);

    int getrlimit(int resource, RLimit rlimit);

    int setrlimit(int resource, RLimit rlimit);

    int open(String pathname, int flags, int mode);

    int close(int fd);

    int fstat64(int fd, Stat64 stats);

    String strerror(int errno);

    int errno();
}
