/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.ffi;

import org.elasticsearch.nativeaccess.lib.Kernel32Library;

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
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.ffi.RuntimeHelper.downcallHandle;

class JdkKernel32Library implements Kernel32Library {

    private static final MethodHandle GetCurrentProcess$mh;
    private static final MethodHandle CloseHandle$mh;
    private static final MethodHandle VirtualLock$mh;
    private static final MethodHandle VirtualQueryEx$mh;
    private static final MethodHandle SetProcessWorkingSetSize$mh;

    static {
        GetCurrentProcess$mh = downcallHandle("GetCurrentProcess", FunctionDescriptor.of(ADDRESS));
        CloseHandle$mh = downcallHandleWithError("CloseHandle", FunctionDescriptor.ofVoid(ADDRESS));
        VirtualLock$mh = downcallHandleWithError("VirtualLock", FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, JAVA_LONG));
        VirtualQueryEx$mh = downcallHandleWithError(
            "VirtualQueryEx",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG));
        SetProcessWorkingSetSize$mh = downcallHandleWithError(
            "SetProcessWorkingSetSize",
            FunctionDescriptor.of(ADDRESS, JAVA_LONG, JAVA_LONG));
    }

    // GetLastError can change from other Java threads so capture it
    private static final StructLayout CAPTURE_GETLASTERROR_LAYOUT = Linker.Option.captureStateLayout();
    private static final Linker.Option CAPTURE_GETLASTERROR_OPTION = Linker.Option.captureCallState("GetLastError");
    private static final VarHandle GetLastError$vh = CAPTURE_GETLASTERROR_LAYOUT.varHandle(groupElement("GetLastError"));

    private static MethodHandle downcallHandleWithError(String function, FunctionDescriptor functionDescriptor) {
        return downcallHandle(function, functionDescriptor, CAPTURE_GETLASTERROR_OPTION);
    }

    static class JdkMemoryBasicInformation implements MemoryBasicInformation {
        private static final MemoryLayout layout = MemoryLayout.structLayout(
            ADDRESS,
            ADDRESS,
            JAVA_LONG,
            JAVA_LONG,
            JAVA_LONG,
            JAVA_LONG,
            JAVA_LONG);
        private static final VarHandle BaseAddress$vh = layout.varHandle(groupElement(0));
        private static final VarHandle AllocationBase$vh = layout.varHandle(groupElement(1));
        private static final VarHandle AllocationProtect$vh = layout.varHandle(groupElement(2));
        private static final VarHandle RegionSize$vh = layout.varHandle(groupElement(3));
        private static final VarHandle State$vh = layout.varHandle(groupElement(4));
        private static final VarHandle Protect$vh = layout.varHandle(groupElement(5));
        private static final VarHandle Type$vh = layout.varHandle(groupElement(6));

        private final MemorySegment segment;

        JdkMemoryBasicInformation() {
            var arena = Arena.ofAuto();
            this.segment = arena.allocate(layout);
        }

        @Override
        public long getBaseAddress() {
            return ((MemorySegment)BaseAddress$vh.get(segment)).address();
        }

        @Override
        public long getAllocationBase() {
            return ((MemorySegment)AllocationBase$vh.get(segment)).address();
        }

        @Override
        public long getAllocationProtect() {
            return (long)AllocationProtect$vh.get(segment);
        }

        @Override
        public long getRegionSize() {
            return (long)RegionSize$vh.get(segment);
        }

        @Override
        public long getState() {
            return (long)State$vh.get(segment);
        }

        @Override
        public long getProtect() {
            return (long)Protect$vh.get(segment);
        }

        @Override
        public long getType() {
            return (long)Type$vh.get(segment);
        }
    }

    private final MemorySegment lastErrorState;

    JdkKernel32Library() {
        Arena arena = Arena.ofShared();
        lastErrorState = arena.allocate(CAPTURE_GETLASTERROR_LAYOUT);
    }

    @Override
    public long GetCurrentProcess() {
        try {
            return (long)GetCurrentProcess$mh.invokeExact();
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean CloseHandle(long handle) {
        try {
            return (boolean)CloseHandle$mh.invokeExact(lastErrorState, MemorySegment.ofAddress(handle));
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int GetLastError() {
        return (int)GetLastError$vh.get(lastErrorState);
    }

    @Override
    public MemoryBasicInformation newMemoryBasicInformation() {
        return new JdkMemoryBasicInformation();
    }

    @Override
    public boolean VirtualLock(long address, long size) {
        try {
            return (boolean)VirtualLock$mh.invokeExact(lastErrorState, MemorySegment.ofAddress(address), size);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int VirtualQueryEx(long processHandle, long address, MemoryBasicInformation memoryInfo) {
        assert memoryInfo instanceof JdkMemoryBasicInformation;
        var jdkMemoryInfo = (JdkMemoryBasicInformation)memoryInfo;
        try {
            return (int)VirtualQueryEx$mh.invokeExact(
                lastErrorState,
                MemorySegment.ofAddress(processHandle),
                MemorySegment.ofAddress(address),
                jdkMemoryInfo.segment,
                jdkMemoryInfo.segment.byteSize());
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean SetProcessWorkingSetSize(long processHandle, long minSize, long maxSize) {
        try {
            return (boolean)SetProcessWorkingSetSize$mh.invokeExact(
                lastErrorState,
                MemorySegment.ofAddress(processHandle),
                minSize,
                maxSize);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
