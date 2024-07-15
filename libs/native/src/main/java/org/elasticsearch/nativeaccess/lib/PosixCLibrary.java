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
     * @see <a href="https://man7.org/linux/man-pages/man3/geteuid.3p.html">geteuid manpage</a>
     */
    int geteuid();

    /** corresponds to struct rlimit */
    interface RLimit {
        long rlim_cur();

        long rlim_max();

        void rlim_cur(long v);

        void rlim_max(long v);
    }

    /**
     * Create a new RLimit struct for use by getrlimit.
     */
    RLimit newRLimit();

    /**
     * Retrieve the current rlimit values for the given resource.
     *
     * @return 0 on success, -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/getrlimit.2.html">getrlimit manpage</a>
     */
    int getrlimit(int resource, RLimit rlimit);

    int setrlimit(int resource, RLimit rlimit);

    /**
     * Lock all the current process's virtual address space into RAM.
     * @param flags flags determining how memory will be locked
     * @return 0 on success, -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/mlock.2.html">mlockall manpage</a>
     */
    int mlockall(int flags);

    /** corresponds to struct stat64 */
    interface Stat64 {
        long st_size();

        long st_blocks();
    }

    Stat64 newStat64(int sizeof, int stSizeOffset, int stBlocksOffset);

    int open(String pathname, int flags, int mode);

    int open(String pathname, int flags);

    int close(int fd);

    int fstat64(int fd, Stat64 stats);

    int ftruncate(int fd, long length);

    interface FStore {
        void set_flags(int flags); /* IN: flags word */

        void set_posmode(int posmode); /* IN: indicates offset field */

        void set_offset(long offset); /* IN: start of the region */

        void set_length(long length); /* IN: size of the region */

        long bytesalloc(); /* OUT: number of bytes allocated */
    }

    FStore newFStore();

    int fcntl(int fd, int cmd, FStore fst);

    /**
     * Return a string description for an error.
     *
     * @param errno The error number
     * @return a String description for the error
     * @see <a href="https://man7.org/linux/man-pages/man3/strerror.3.html">strerror manpage</a>
     */
    String strerror(int errno);

    /**
     * Return the error number from the last failed C library call.
     *
     * @see <a href="https://man7.org/linux/man-pages/man3/errno.3.html">errno manpage</a>
     */
    int errno();
}
