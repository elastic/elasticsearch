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

    /**
     * Lock all the current process's virtual address space into RAM.
     * @param flags flags determining how memory will be locked
     * @return 0 on success, -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/mlock.2.html">mlockall manpage</a>
     */
    int mlockall(int flags);

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
