/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.WindowsFunctions.ConsoleCtrlHandler;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.util.function.IntConsumer;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.MemoryLayout.paddingLayout;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_CHAR;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.upcallHandle;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.upcallStub;
import static org.elasticsearch.nativeaccess.jdk.MemorySegmentUtil.varHandleWithoutOffset;

class JdkKernel32Library implements Kernel32Library {
    static {
        System.loadLibrary("kernel32");
    }

    // GetLastError can change from other Java threads so capture it
    private static final StructLayout CAPTURE_GETLASTERROR_LAYOUT = Linker.Option.captureStateLayout();
    private static final Linker.Option CAPTURE_GETLASTERROR_OPTION = Linker.Option.captureCallState("GetLastError");
    private static final VarHandle GetLastError$vh = varHandleWithoutOffset(CAPTURE_GETLASTERROR_LAYOUT, groupElement("GetLastError"));

    private static final MethodHandle GetCurrentProcess$mh = downcallHandle("GetCurrentProcess", FunctionDescriptor.of(ADDRESS));
    private static final MethodHandle CloseHandle$mh = downcallHandleWithError("CloseHandle", FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS));
    private static final MethodHandle VirtualLock$mh = downcallHandleWithError(
        "VirtualLock",
        FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, JAVA_LONG)
    );
    private static final MethodHandle VirtualQueryEx$mh = downcallHandleWithError(
        "VirtualQueryEx",
        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG)
    );
    private static final MethodHandle SetProcessWorkingSetSize$mh = downcallHandleWithError(
        "SetProcessWorkingSetSize",
        FunctionDescriptor.of(ADDRESS, JAVA_LONG, JAVA_LONG)
    );
    private static final MethodHandle GetCompressedFileSizeW$mh = downcallHandleWithError(
        "GetCompressedFileSizeW",
        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS)
    );
    private static final MethodHandle GetShortPathNameW$mh = downcallHandleWithError(
        "GetShortPathNameW",
        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT)
    );
    private static final MethodHandle SetConsoleCtrlHandler$mh = downcallHandleWithError(
        "SetConsoleCtrlHandler",
        FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, JAVA_BOOLEAN)
    );

    private static final FunctionDescriptor ConsoleCtrlHandler_handle$fd = FunctionDescriptor.of(JAVA_BOOLEAN, JAVA_INT);
    private static final MethodHandle ConsoleCtrlHandler_handle$mh = upcallHandle(
        ConsoleCtrlHandler.class,
        "handle",
        ConsoleCtrlHandler_handle$fd
    );
    private static final MethodHandle CreateJobObjectW$mh = downcallHandleWithError(
        "CreateJobObjectW",
        FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS)
    );
    private static final MethodHandle AssignProcessToJobObject$mh = downcallHandleWithError(
        "AssignProcessToJobObject",
        FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, ADDRESS)
    );
    private static final MethodHandle QueryInformationJobObject$mh = downcallHandleWithError(
        "QueryInformationJobObject",
        FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS)
    );
    private static final MethodHandle SetInformationJobObject$mh = downcallHandleWithError(
        "SetInformationJobObject",
        FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT)
    );

    private static MethodHandle downcallHandleWithError(String function, FunctionDescriptor functionDescriptor) {
        return downcallHandle(function, functionDescriptor, CAPTURE_GETLASTERROR_OPTION);
    }

    static class JdkHandle implements Handle {
        MemorySegment address;

        JdkHandle(MemorySegment address) {
            this.address = address;
        }
    }

    static class JdkAddress implements Address {
        MemorySegment address;

        JdkAddress(MemorySegment address) {
            this.address = address;
        }

        @Override
        public Address add(long offset) {
            return new JdkAddress(MemorySegment.ofAddress(address.address()));
        }
    }

    static class JdkMemoryBasicInformation implements MemoryBasicInformation {
        private static final MemoryLayout layout = MemoryLayout.structLayout(
            ADDRESS,
            paddingLayout(16),
            JAVA_LONG,
            JAVA_LONG,
            JAVA_LONG,
            JAVA_LONG
        );
        private static final VarHandle BaseAddress$vh = varHandleWithoutOffset(layout, groupElement(0));
        private static final VarHandle RegionSize$vh = varHandleWithoutOffset(layout, groupElement(2));
        private static final VarHandle State$vh = varHandleWithoutOffset(layout, groupElement(3));
        private static final VarHandle Protect$vh = varHandleWithoutOffset(layout, groupElement(4));
        private static final VarHandle Type$vh = varHandleWithoutOffset(layout, groupElement(5));

        private final MemorySegment segment;

        JdkMemoryBasicInformation() {
            this.segment = Arena.ofAuto().allocate(layout);
            this.segment.fill((byte) 0);
        }

        @Override
        public Address BaseAddress() {
            return new JdkAddress((MemorySegment) BaseAddress$vh.get(segment));
        }

        @Override
        public long RegionSize() {
            return (long) RegionSize$vh.get(segment);
        }

        @Override
        public long State() {
            return (long) State$vh.get(segment);
        }

        @Override
        public long Protect() {
            return (long) Protect$vh.get(segment);
        }

        @Override
        public long Type() {
            return (long) Type$vh.get(segment);
        }
    }

    static class JdkJobObjectBasicLimitInformation implements JobObjectBasicLimitInformation {
        private static final MemoryLayout layout = MemoryLayout.structLayout(
            paddingLayout(16),
            JAVA_INT,
            paddingLayout(20),
            JAVA_INT,
            paddingLayout(20)
        ).withByteAlignment(8);

        private static final VarHandle LimitFlags$vh = varHandleWithoutOffset(layout, groupElement(1));
        private static final VarHandle ActiveProcessLimit$vh = varHandleWithoutOffset(layout, groupElement(3));

        private final MemorySegment segment;

        JdkJobObjectBasicLimitInformation() {
            var arena = Arena.ofAuto();
            this.segment = arena.allocate(layout);
            segment.fill((byte) 0);
        }

        @Override
        public void setLimitFlags(int v) {
            LimitFlags$vh.set(segment, v);
        }

        @Override
        public void setActiveProcessLimit(int v) {
            ActiveProcessLimit$vh.set(segment, v);
        }
    }

    private final MemorySegment lastErrorState;

    JdkKernel32Library() {
        Arena arena = Arena.ofAuto();
        lastErrorState = arena.allocate(CAPTURE_GETLASTERROR_LAYOUT);
    }

    @Override
    public Handle GetCurrentProcess() {
        try {
            return new JdkHandle((MemorySegment) GetCurrentProcess$mh.invokeExact());
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean CloseHandle(Handle handle) {
        assert handle instanceof JdkHandle;
        var jdkHandle = (JdkHandle) handle;

        try {
            return (boolean) CloseHandle$mh.invokeExact(lastErrorState, jdkHandle.address);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int GetLastError() {
        return (int) GetLastError$vh.get(lastErrorState);
    }

    @Override
    public MemoryBasicInformation newMemoryBasicInformation() {
        return new JdkMemoryBasicInformation();
    }

    @Override
    public boolean VirtualLock(Address address, long size) {
        assert address instanceof JdkAddress;
        var jdkAddress = (JdkAddress) address;

        try {
            return (boolean) VirtualLock$mh.invokeExact(lastErrorState, jdkAddress.address, size);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int VirtualQueryEx(Handle process, Address address, MemoryBasicInformation memoryInfo) {
        assert process instanceof JdkHandle;
        assert address instanceof JdkAddress;
        assert memoryInfo instanceof JdkMemoryBasicInformation;
        var jdkProcess = (JdkHandle) process;
        var jdkAddress = (JdkAddress) address;
        var jdkMemoryInfo = (JdkMemoryBasicInformation) memoryInfo;

        try {
            return (int) VirtualQueryEx$mh.invokeExact(
                lastErrorState,
                jdkProcess.address,
                jdkAddress.address,
                jdkMemoryInfo.segment,
                jdkMemoryInfo.segment.byteSize()
            );
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean SetProcessWorkingSetSize(Handle process, long minSize, long maxSize) {
        assert process instanceof JdkHandle;
        var jdkProcess = (JdkHandle) process;
        try {
            return (boolean) SetProcessWorkingSetSize$mh.invokeExact(lastErrorState, jdkProcess.address, minSize, maxSize);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int GetCompressedFileSizeW(String lpFileName, IntConsumer lpFileSizeHigh) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment wideFileName = ArenaUtil.allocateFrom(arena, lpFileName + "\0", StandardCharsets.UTF_16LE);
            MemorySegment fileSizeHigh = arena.allocate(JAVA_INT);

            int ret = (int) GetCompressedFileSizeW$mh.invokeExact(lastErrorState, wideFileName, fileSizeHigh);
            lpFileSizeHigh.accept(fileSizeHigh.get(JAVA_INT, 0));
            return ret;
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int GetShortPathNameW(String lpszLongPath, char[] lpszShortPath, int cchBuffer) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment wideFileName = ArenaUtil.allocateFrom(arena, lpszLongPath + "\0", StandardCharsets.UTF_16LE);
            MemorySegment shortPath;
            if (lpszShortPath != null) {
                shortPath = ArenaUtil.allocate(arena, JAVA_CHAR, cchBuffer);
            } else {
                shortPath = MemorySegment.NULL;
            }

            int ret = (int) GetShortPathNameW$mh.invokeExact(lastErrorState, wideFileName, shortPath, cchBuffer);
            if (shortPath != MemorySegment.NULL) {
                for (int i = 0; i < cchBuffer; ++i) {
                    lpszShortPath[i] = shortPath.getAtIndex(JAVA_CHAR, i);
                }
            }
            return ret;
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean SetConsoleCtrlHandler(ConsoleCtrlHandler handler, boolean add) {
        // use the global arena so the handler will have the lifetime of the jvm
        MemorySegment nativeHandler = upcallStub(ConsoleCtrlHandler_handle$mh, handler, ConsoleCtrlHandler_handle$fd, Arena.global());
        try {
            return (boolean) SetConsoleCtrlHandler$mh.invokeExact(lastErrorState, nativeHandler, add);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public Handle CreateJobObjectW() {
        try {
            return new JdkHandle((MemorySegment) CreateJobObjectW$mh.invokeExact(lastErrorState, MemorySegment.NULL, MemorySegment.NULL));
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean AssignProcessToJobObject(Handle job, Handle process) {
        assert job instanceof JdkHandle;
        assert process instanceof JdkHandle;
        var jdkJob = (JdkHandle) job;
        var jdkProcess = (JdkHandle) process;

        try {
            return (boolean) AssignProcessToJobObject$mh.invokeExact(lastErrorState, jdkJob.address, jdkProcess.address);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public JobObjectBasicLimitInformation newJobObjectBasicLimitInformation() {
        return new JdkJobObjectBasicLimitInformation();
    }

    @Override
    public boolean QueryInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        assert job instanceof JdkHandle;
        assert info instanceof JdkJobObjectBasicLimitInformation;
        var jdkJob = (JdkHandle) job;
        var jdkInfo = (JdkJobObjectBasicLimitInformation) info;

        try {
            return (boolean) QueryInformationJobObject$mh.invokeExact(
                lastErrorState,
                jdkJob.address,
                infoClass,
                jdkInfo.segment,
                (int) jdkInfo.segment.byteSize(),
                MemorySegment.NULL
            );
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean SetInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        assert job instanceof JdkHandle;
        assert info instanceof JdkJobObjectBasicLimitInformation;
        var jdkJob = (JdkHandle) job;
        var jdkInfo = (JdkJobObjectBasicLimitInformation) info;

        try {
            return (boolean) SetInformationJobObject$mh.invokeExact(
                lastErrorState,
                jdkJob.address,
                infoClass,
                jdkInfo.segment,
                (int) jdkInfo.segment.byteSize()
            );
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
