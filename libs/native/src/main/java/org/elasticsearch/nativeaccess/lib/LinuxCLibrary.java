/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

public non-sealed interface LinuxCLibrary extends NativeLibrary {

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
}
