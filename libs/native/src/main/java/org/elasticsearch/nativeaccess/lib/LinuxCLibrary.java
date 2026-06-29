/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

public interface LinuxCLibrary {

    /**
     * Corresponds to struct sock_filter
     * @param code insn
     * @param jt number of insn to jump (skip) if true
     * @param jf number of insn to jump (skip) if false
     * @param k additional data
     */
    record SockFilter(short code, byte jt, byte jf, int k) {}

    interface SockFProg {
        long address();
    }

    SockFProg newSockFProg(SockFilter filters[]);

    /**
     * maps to prctl(2)
     */
    int prctl(int option, long arg2, long arg3, long arg4, long arg5);

    /**
     * used to call seccomp(2), its too new...
     * this is the only way, DON'T use it on some other architecture unless you know wtf you are doing
     */
    long syscall(long number, int operation, int flags, long address);

    int fallocate(int fd, int mode, long offset, long length);

    /**
     * maps to sync_file_range(2) — Linux-only
     * Initiates/waits for writeback of a byte range of a file to disk.
     * @param fd     file descriptor opened with write access
     * @param offset byte offset of the range start
     * @param nbytes length of the range (0 means "to end of file")
     * @param flags  combination of SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER
     * @return 0 on success, -1 on error with errno set
     */
    int syncFileRange(int fd, long offset, long nbytes, int flags);
}
