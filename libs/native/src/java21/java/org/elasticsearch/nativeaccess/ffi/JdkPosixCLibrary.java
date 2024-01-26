/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.ffi;

import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.ffi.RuntimeHelper.downcallHandle;

class JdkPosixCLibrary implements PosixCLibrary {

    // errno can change between system calls, so we capture it
    private static final StructLayout CAPTURE_ERRNO_LAYOUT = Linker.Option.captureStateLayout();
    private static final Linker.Option CAPTURE_ERRNO_OPTION = Linker.Option.captureCallState("errno");
    private static final VarHandle errno$vh = CAPTURE_ERRNO_LAYOUT.varHandle(groupElement("errno"));

    private static final MethodHandle strerror$mh;
    private static final MethodHandle geteuid$mh;
    private static final MethodHandle mlockall$mh;
    private static final MethodHandle getrlimit$mh;
    private static final MethodHandle setrlimit$mh;

    static {
        Arena arena = Arena.ofAuto();
        errnoState = arena.allocate(CAPTURE_ERRNO_LAYOUT);

        strerror$mh = downcallHandle("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT));
        geteuid$mh = downcallHandle("geteuid", FunctionDescriptor.of(JAVA_INT));
        mlockall$mh = downcallHandleWithErrno("mlockall", FunctionDescriptor.of(JAVA_INT, JAVA_INT));
        var rlimitDesc = FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS);
        getrlimit$mh = downcallHandleWithErrno("getrlimit", rlimitDesc);
        setrlimit$mh = downcallHandleWithErrno("setrlimit", rlimitDesc);
    }

    static MethodHandle downcallHandleWithErrno(String function, FunctionDescriptor functionDescriptor) {
        return downcallHandle(function, functionDescriptor, CAPTURE_ERRNO_OPTION);
    }

    static class JdkRLimit implements RLimit {
        private static final MemoryLayout layout = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG);
        private static final VarHandle rlim_cur$vh = layout.varHandle(groupElement(0));
        private static final VarHandle rlim_max$vh = layout.varHandle(groupElement(1));

        private final MemorySegment segment;

        JdkRLimit() {
            var arena = Arena.ofAuto();
            this.segment = arena.allocate(layout);
        }

        @Override
        public long rlim_cur() {
            return (long)rlim_cur$vh.get(segment);
        }

        @Override
        public long rlim_max() {
            return (long)rlim_max$vh.get(segment);
        }

        @Override
        public void rlim_cur(long v) {
            rlim_cur$vh.set(segment, v);
        }

        @Override
        public void rlim_max(long v) {
            rlim_max$vh.set(segment, v);
        }
    }

    static final MemorySegment errnoState;

    @Override
    public int errno() {
        return (int)errno$vh.get(errnoState);
    }

    @Override
    public String strerror(int errno) {
        try {
            MemorySegment str = (MemorySegment)strerror$mh.invokeExact(errno);
            return str.reinterpret(Long.MAX_VALUE).getUtf8String(0);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int geteuid() {
        try {
            return (int) geteuid$mh.invokeExact();
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int mlockall(int flags) {
        try {
            return (int)mlockall$mh.invokeExact(flags, errnoState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public RLimit newRLimit() {
        return new JdkRLimit();
    }

    @Override
    public int getrlimit(int resource, RLimit rlimit) {
        assert rlimit instanceof JdkRLimit;
        var jdkRlimit = (JdkRLimit)rlimit;
        try {
            return (int)getrlimit$mh.invokeExact(resource, jdkRlimit.segment, errnoState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int setrlimit(int resource, RLimit rlimit) {
        assert rlimit instanceof JdkRLimit;
        var jdkRlimit = (JdkRLimit)rlimit;
        try {
            return (int)setrlimit$mh.invokeExact(resource, jdkRlimit.segment, errnoState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
