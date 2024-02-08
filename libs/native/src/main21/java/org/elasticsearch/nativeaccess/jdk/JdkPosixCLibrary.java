/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

class JdkPosixCLibrary implements PosixCLibrary {

    private static final Logger logger = LogManager.getLogger(JdkPosixCLibrary.class);

    // errno can change between system calls, so we capture it
    private static final StructLayout CAPTURE_ERRNO_LAYOUT = Linker.Option.captureStateLayout();
    static final Linker.Option CAPTURE_ERRNO_OPTION = Linker.Option.captureCallState("errno");
    private static final VarHandle errno$vh = CAPTURE_ERRNO_LAYOUT.varHandle(groupElement("errno"));

    private static final MethodHandle geteuid$mh = downcallHandle("geteuid", FunctionDescriptor.of(JAVA_INT));
    private static final MethodHandle strerror$mh;
    private static final MethodHandle mlockall$mh;
    private static final MethodHandle getrlimit$mh;
    private static final MethodHandle setrlimit$mh;
    private static final MethodHandle open$mh;
    private static final MethodHandle close$mh;
    private static final MethodHandle fstat64$mh;

    static {
        Arena arena = Arena.ofAuto();
        errnoState = arena.allocate(CAPTURE_ERRNO_LAYOUT);

        strerror$mh = downcallHandle("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT));
        mlockall$mh = downcallHandleWithErrno("mlockall", FunctionDescriptor.of(JAVA_INT, JAVA_INT));
        var rlimitDesc = FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS);
        getrlimit$mh = downcallHandleWithErrno("getrlimit", rlimitDesc);
        setrlimit$mh = downcallHandleWithErrno("setrlimit", rlimitDesc);
        open$mh = downcallHandle(
            "open",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT),
            CAPTURE_ERRNO_OPTION,
            Linker.Option.firstVariadicArg(2)
        );
        close$mh = downcallHandleWithErrno("close", FunctionDescriptor.of(JAVA_INT, JAVA_INT));

        MethodHandle fstat;
        try {
            fstat = downcallHandleWithErrno("fstat64", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS));
        } catch (LinkageError e) {
            // Due to different sizes of the stat structure for 32 vs 64 bit machines, on some systems fstat actually points to
            // an internal symbol. So we fall back to looking for that symbol.
            fstat = downcallHandleWithErrno("__fxstat64", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS));
        }
        fstat64$mh = fstat;
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
            return (long) rlim_cur$vh.get(segment);
        }

        @Override
        public long rlim_max() {
            return (long) rlim_max$vh.get(segment);
        }

        @Override
        public void rlim_cur(long v) {
            rlim_cur$vh.set(segment, v);
        }

        @Override
        public void rlim_max(long v) {
            rlim_max$vh.set(segment, v);
        }

        @Override
        public String toString() {
            return "JdkRLimit[rlim_cur=" + rlim_cur() + ", rlim_max=" + rlim_max();
        }
    }

    class JdkStat64 implements Stat64 {

        private final MemorySegment segment;
        private final int stSizeOffset;

        JdkStat64(int sizeof, int stSizeOffset) {
            var arena = Arena.ofAuto();
            this.segment = arena.allocate(sizeof, 8);
            this.stSizeOffset = stSizeOffset;
        }

        @Override
        public long st_size() {
            return segment.get(JAVA_LONG, stSizeOffset);
        }
    }

    static final MemorySegment errnoState;

    @Override
    public int errno() {
        return (int) errno$vh.get(errnoState);
    }

    @Override
    public String strerror(int errno) {
        try {
            MemorySegment str = (MemorySegment) strerror$mh.invokeExact(errno);
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
            return (int) mlockall$mh.invokeExact(errnoState, flags);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public RLimit newRLimit() {
        return new JdkRLimit();
    }

    @Override
    public Stat64 newStat64(int sizeof, int stSizeOffset) {
        return null;
    }

    @Override
    public int getrlimit(int resource, RLimit rlimit) {
        assert rlimit instanceof JdkRLimit;
        var jdkRlimit = (JdkRLimit) rlimit;
        try {
            return (int) getrlimit$mh.invokeExact(errnoState, resource, jdkRlimit.segment);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int setrlimit(int resource, RLimit rlimit) {
        assert rlimit instanceof JdkRLimit;
        var jdkRlimit = (JdkRLimit) rlimit;
        try {
            return (int) setrlimit$mh.invokeExact(errnoState, resource, jdkRlimit.segment);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int open(String pathname, int flags, int mode) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativePathname = arena.allocateUtf8String(pathname);
            return (int) open$mh.invokeExact(errnoState, nativePathname, flags, mode);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int close(int fd) {
        try {
            return (int) close$mh.invokeExact(errnoState, fd);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int fstat64(int fd, Stat64 stat64) {
        assert stat64 instanceof JdkStat64;
        var jdkStat = (JdkStat64) stat64;
        try {
            return (int) fstat64$mh.invokeExact(errnoState, fd, jdkStat.segment);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
