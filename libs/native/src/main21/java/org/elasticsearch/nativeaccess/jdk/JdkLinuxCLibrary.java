/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.lib.LinuxCLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.MemoryLayout.paddingLayout;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;
import static org.elasticsearch.nativeaccess.jdk.JdkPosixCLibrary.CAPTURE_ERRNO_OPTION;
import static org.elasticsearch.nativeaccess.jdk.JdkPosixCLibrary.downcallHandleWithErrno;
import static org.elasticsearch.nativeaccess.jdk.JdkPosixCLibrary.errnoState;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

class JdkLinuxCLibrary implements LinuxCLibrary {
    private static final MethodHandle prctl$mh;
    static {
        try {
            prctl$mh = downcallHandleWithErrno(
                "prctl",
                FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG)
            );
        } catch (UnsatisfiedLinkError e) {
            throw new UnsupportedOperationException(
                "seccomp unavailable: could not link methods. requires kernel 3.5+ "
                    + "with CONFIG_SECCOMP and CONFIG_SECCOMP_FILTER compiled in"
            );
        }
    }
    private static final MethodHandle syscall$mh = downcallHandle(
        "syscall",
        FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, JAVA_INT, JAVA_INT, JAVA_LONG),
        CAPTURE_ERRNO_OPTION,
        Linker.Option.firstVariadicArg(1)
    );
    private static final MethodHandle fallocate$mh = downcallHandleWithErrno(
        "fallocate",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG)
    );

    private static class JdkSockFProg implements SockFProg {
        private static final MemoryLayout layout = MemoryLayout.structLayout(JAVA_SHORT, paddingLayout(6), ADDRESS);

        private final MemorySegment segment;

        JdkSockFProg(SockFilter filters[]) {
            Arena arena = Arena.ofAuto();
            this.segment = arena.allocate(layout);
            var instSegment = arena.allocate(filters.length * 8L);
            segment.set(JAVA_SHORT, 0, (short) filters.length);
            segment.set(ADDRESS, 8, instSegment);

            int offset = 0;
            for (SockFilter f : filters) {
                instSegment.set(JAVA_SHORT, offset, f.code());
                instSegment.set(JAVA_BYTE, offset + 2, f.jt());
                instSegment.set(JAVA_BYTE, offset + 3, f.jf());
                instSegment.set(JAVA_INT, offset + 4, f.k());
                offset += 8;
            }
        }

        @Override
        public long address() {
            return segment.address();
        }
    }

    @Override
    public SockFProg newSockFProg(SockFilter[] filters) {
        return new JdkSockFProg(filters);
    }

    @Override
    public int prctl(int option, long arg2, long arg3, long arg4, long arg5) {
        try {
            return (int) prctl$mh.invokeExact(errnoState, option, arg2, arg3, arg4, arg5);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long syscall(long number, int operation, int flags, long address) {
        try {
            return (long) syscall$mh.invokeExact(errnoState, number, operation, flags, address);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int fallocate(int fd, int mode, long offset, long length) {
        try {
            return (int) fallocate$mh.invokeExact(errnoState, fd, mode, offset, length);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
