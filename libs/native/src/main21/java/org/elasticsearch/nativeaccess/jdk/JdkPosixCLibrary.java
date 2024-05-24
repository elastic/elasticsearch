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
import static org.elasticsearch.nativeaccess.jdk.MemorySegmentUtil.varHandleWithoutOffset;

class JdkPosixCLibrary implements PosixCLibrary {

    private static final Logger logger = LogManager.getLogger(JdkPosixCLibrary.class);

    // errno can change between system calls, so we capture it
    private static final StructLayout CAPTURE_ERRNO_LAYOUT = Linker.Option.captureStateLayout();
    static final Linker.Option CAPTURE_ERRNO_OPTION = Linker.Option.captureCallState("errno");
    private static final VarHandle errno$vh = varHandleWithoutOffset(CAPTURE_ERRNO_LAYOUT, groupElement("errno"));

    private static final MethodHandle geteuid$mh = downcallHandle("geteuid", FunctionDescriptor.of(JAVA_INT));
    private static final MethodHandle strerror$mh = downcallHandle("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT));
    private static final MethodHandle getrlimit$mh = downcallHandleWithErrno(
        "getrlimit",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS)
    );
    private static final MethodHandle mlockall$mh = downcallHandleWithErrno("mlockall", FunctionDescriptor.of(JAVA_INT, JAVA_INT));

    static final MemorySegment errnoState = Arena.ofAuto().allocate(CAPTURE_ERRNO_LAYOUT);

    static MethodHandle downcallHandleWithErrno(String function, FunctionDescriptor functionDescriptor) {
        return downcallHandle(function, functionDescriptor, CAPTURE_ERRNO_OPTION);
    }

    @Override
    public int errno() {
        return (int) errno$vh.get(errnoState);
    }

    @Override
    public String strerror(int errno) {
        try {
            MemorySegment str = (MemorySegment) strerror$mh.invokeExact(errno);
            return MemorySegmentUtil.getString(str.reinterpret(Long.MAX_VALUE), 0);
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
    public RLimit newRLimit() {
        return new JdkRLimit();
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
    public int mlockall(int flags) {
        try {
            return (int) mlockall$mh.invokeExact(errnoState, flags);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    static class JdkRLimit implements RLimit {
        private static final MemoryLayout layout = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG);
        private static final VarHandle rlim_cur$vh = varHandleWithoutOffset(layout, groupElement(0));
        private static final VarHandle rlim_max$vh = varHandleWithoutOffset(layout, groupElement(1));

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
        public String toString() {
            return "JdkRLimit[rlim_cur=" + rlim_cur() + ", rlim_max=" + rlim_max();
        }
    }
}
