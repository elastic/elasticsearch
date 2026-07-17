/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.Addressable;
import org.elasticsearch.foreign.ArrayField;
import org.elasticsearch.foreign.CaptureErrno;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.StructFactory;
import org.elasticsearch.foreign.StructSpecification;
import org.elasticsearch.foreign.Variadic;

@LibrarySpecification
public interface LinuxCLibrary {

    /**
     * Corresponds to struct sock_filter
     * @param code insn
     * @param jt number of insn to jump (skip) if true
     * @param jf number of insn to jump (skip) if false
     * @param k additional data
     */
    @StructSpecification
    record SockFilter(short code, byte jt, byte jf, int k) {}

    /** struct sock_fprog { __u16 len; struct sock_filter *filter; } */
    @StructSpecification
    interface SockFProg extends Addressable {
        short len();

        @ArrayField(lengthField = "len")
        SockFilter filter(int index);
    }

    @StructFactory
    SockFProg newSockFProg(SockFilter[] filters);

    /**
     * maps to prctl(2)
     */
    @CaptureErrno
    @Function("prctl")
    int prctl(int option, long arg2, long arg3, long arg4, long arg5);

    /**
     * used to call seccomp(2), its too new...
     * this is the only way, DON'T use it on some other architecture unless you know wtf you are doing
     */
    @CaptureErrno
    @Variadic(firstArg = 1)
    @Function("syscall")
    long syscall(long number, int operation, int flags, Addressable address);

    @CaptureErrno
    @Function("fallocate")
    int fallocate(int fd, int mode, long offset, long length);
}
