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
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public class JdkPosixCLibrary implements PosixCLibrary {

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup SYMBOL_LOOKUP = LINKER.defaultLookup();

    // errno can change between system calls
    private static final StructLayout CAPTURE_ERRNO_LAYOUT = Linker.Option.captureStateLayout();
    private static final Linker.Option CAPTURE_ERRNO_OPTION = Linker.Option.captureCallState("errno");
    private static final VarHandle ERRNO_HANDLE = CAPTURE_ERRNO_LAYOUT.varHandle(groupElement("errno"));

    private static final MethodHandle strerror$mh;
    private static final MethodHandle geteuid$mh;
    private static final MethodHandle mlockall$mh;
    private static final MethodHandle getrlimit$mh;
    private static final MethodHandle setrlimit$mh;

    private static final MemoryLayout rlimitLayout = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG);

    static {
        strerror$mh = downcallHandle("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT));
        geteuid$mh = downcallHandle("geteuid", FunctionDescriptor.of(JAVA_INT));
        mlockall$mh = downcallHandleWithErrno("mlockall", FunctionDescriptor.of(JAVA_INT, JAVA_INT));
        var rlimitDesc = FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS);
        getrlimit$mh = downcallHandleWithErrno("getrlimit", rlimitDesc);
        setrlimit$mh = downcallHandleWithErrno("setrlimit", rlimitDesc);
    }

    private static MemorySegment functionAddress(String function) {
        return SYMBOL_LOOKUP.find(function).orElseThrow(
            () -> new LinkageError("Function " + function + " could not be found in libc"));
    }
    private static MethodHandle downcallHandle(String function, FunctionDescriptor functionDescriptor) {
        return LINKER.downcallHandle(functionAddress(function), functionDescriptor);
    }

    private static MethodHandle downcallHandleWithErrno(String function, FunctionDescriptor functionDescriptor) {
        return LINKER.downcallHandle(functionAddress(function), functionDescriptor, CAPTURE_ERRNO_OPTION);
    }

    private int lastErrno;

    @Override
    public int errno() {
        return lastErrno;
    }

    @Override
    public String strerror(int errno) {
        try {
            MemorySegment str = (MemorySegment)strerror$mh.invokeExact(errno);
            return str.getUtf8String(0);
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
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment capturedState = arena.allocate(CAPTURE_ERRNO_LAYOUT);
            int ret = (int)mlockall$mh.invokeExact(flags);
            lastErrno = (int)ERRNO_HANDLE.get(capturedState);
            return ret;
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int getrlimit(int resource, RLimit rlimit) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment capturedState = arena.allocate(CAPTURE_ERRNO_LAYOUT);
            MemorySegment segment = arena.allocate(rlimitLayout);

            int ret = (int)getrlimit$mh.invokeExact(capturedState, resource, segment);
            lastErrno = (int)ERRNO_HANDLE.get(capturedState);

            rlimit.rlim_cur = segment.getAtIndex(JAVA_LONG, 0);
            rlimit.rlim_max = segment.getAtIndex(JAVA_LONG, 1);

            return ret;
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int setrlimit(int resource, RLimit rlimit) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment capturedState = arena.allocate(CAPTURE_ERRNO_LAYOUT);
            MemorySegment segment = arena.allocate(rlimitLayout);

            segment.setAtIndex(JAVA_LONG, 0, rlimit.rlim_cur);
            segment.setAtIndex(JAVA_LONG, 1, rlimit.rlim_max);

            int ret = (int)getrlimit$mh.invokeExact(resource, segment);
            lastErrno = (int)ERRNO_HANDLE.get(capturedState);

            return ret;
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
