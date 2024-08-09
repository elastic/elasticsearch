/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;

/**
 * Provides access to methods in libc.so available on POSIX systems.
 */
public non-sealed interface PosixCLibrary extends NativeLibrary {

    /** socket domain indicating unix file socket */
    short AF_UNIX = 1;

    /** socket type indicating a datagram-oriented socket */
    int SOCK_DGRAM = 2;

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
     * Open a file descriptor to connect to a socket.
     *
     * @param domain The socket protocol family, eg AF_UNIX
     * @param type The socket type, eg SOCK_DGRAM
     * @param protocol The protocol for the given protocl family, normally 0
     * @return an open file descriptor, or -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/socket.2.html">socket manpage</a>
     */
    int socket(int domain, int type, int protocol);

    /**
     * Marker interface for sockaddr struct implementations.
     */
    interface SockAddr {}

    /**
     * Create a sockaddr for the AF_UNIX family.
     */
    SockAddr newUnixSockAddr(String path);

    /**
     * Connect a socket to an address.
     *
     * @param sockfd An open socket file descriptor
     * @param addr The address to connect to
     * @return 0 on success, -1 on failure with errno set
     */
    int connect(int sockfd, SockAddr addr);

    /**
     * Send a message to a socket.
     *
     * @param sockfd The open socket file descriptor
     * @param buffer The message bytes to send
     * @param flags Flags that may adjust how the message is sent
     * @return The number of bytes sent, or -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/sendto.2.html">send manpage</a>
     */
    long send(int sockfd, CloseableByteBuffer buffer, int flags);

    /**
     * Close a file descriptor
     * @param fd The file descriptor to close
     * @return 0 on success, -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/close.2.html">close manpage</a>
     */
    int close(int fd);

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
