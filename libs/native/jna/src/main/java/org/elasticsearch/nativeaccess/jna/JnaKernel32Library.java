/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.IntegerType;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.Structure.ByReference;
import com.sun.jna.WString;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.win32.StdCallLibrary;

import org.elasticsearch.nativeaccess.WindowsFunctions.ConsoleCtrlHandler;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;

import java.util.List;
import java.util.function.IntConsumer;

class JnaKernel32Library implements Kernel32Library {
    private static class JnaHandle implements Handle {
        final Pointer pointer;

        JnaHandle(Pointer pointer) {
            this.pointer = pointer;
        }
    }

    static class JnaAddress implements Address {
        final Pointer pointer;

        JnaAddress(Pointer pointer) {
            this.pointer = pointer;
        }

        @Override
        public Address add(long offset) {
            return new JnaAddress(new Pointer(Pointer.nativeValue(pointer) + offset));
        }
    }

    public static class SizeT extends IntegerType {
        // JNA requires this no-arg constructor to be public,
        // otherwise it fails to register kernel32 library
        public SizeT() {
            this(0);
        }

        public SizeT(long value) {
            super(Native.SIZE_T_SIZE, value);
        }
    }

    /**
     * @see MemoryBasicInformation
     */
    public static class JnaMemoryBasicInformation extends Structure implements MemoryBasicInformation {
        // note: these members must be public for jna to set them
        public Pointer BaseAddress = new Pointer(0);
        public byte[] _ignore = new byte[16];
        public SizeT RegionSize = new SizeT();
        public NativeLong State;
        public NativeLong Protect;
        public NativeLong Type;

        @Override
        protected List<String> getFieldOrder() {
            return List.of("BaseAddress", "_ignore", "RegionSize", "State", "Protect", "Type");
        }

        @Override
        public Address BaseAddress() {
            return new JnaAddress(BaseAddress);
        }

        @Override
        public long RegionSize() {
            return RegionSize.longValue();
        }

        @Override
        public long State() {
            return State.longValue();
        }

        @Override
        public long Protect() {
            return Protect.longValue();
        }

        @Override
        public long Type() {
            return Type.longValue();
        }
    }

    /**
     * Basic limit information for a job object
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms684147%28v=vs.85%29.aspx
     */
    public static class JnaJobObjectBasicLimitInformation extends Structure implements ByReference, JobObjectBasicLimitInformation {
        public byte[] _ignore1 = new byte[16];
        public int LimitFlags;
        public byte[] _ignore2 = new byte[20];
        public int ActiveProcessLimit;
        public byte[] _ignore3 = new byte[20];

        public JnaJobObjectBasicLimitInformation() {
            super(8);
        }

        @Override
        protected List<String> getFieldOrder() {
            return List.of("_ignore1", "LimitFlags", "_ignore2", "ActiveProcessLimit", "_ignore3");
        }

        @Override
        public void setLimitFlags(int v) {
            LimitFlags = v;
        }

        @Override
        public void setActiveProcessLimit(int v) {
            ActiveProcessLimit = v;
        }
    }

    /**
     * JNA adaptation of {@link ConsoleCtrlHandler}
     */
    public static class NativeHandlerCallback implements StdCallLibrary.StdCallCallback {

        private final ConsoleCtrlHandler handler;

        public NativeHandlerCallback(ConsoleCtrlHandler handler) {
            this.handler = handler;
        }

        public boolean callback(long dwCtrlType) {
            return handler.handle((int) dwCtrlType);
        }
    }

    private interface NativeFunctions extends StdCallLibrary {
        Pointer GetCurrentProcess();

        boolean CloseHandle(Pointer handle);

        boolean VirtualLock(Pointer address, SizeT size);

        int VirtualQueryEx(Pointer handle, Pointer address, JnaMemoryBasicInformation memoryInfo, int length);

        boolean SetProcessWorkingSetSize(Pointer handle, SizeT minSize, SizeT maxSize);

        int GetCompressedFileSizeW(WString lpFileName, IntByReference lpFileSizeHigh);

        int GetShortPathNameW(WString lpszLongPath, char[] lpszShortPath, int cchBuffer);

        boolean SetConsoleCtrlHandler(StdCallLibrary.StdCallCallback handler, boolean add);

        Pointer CreateJobObjectW(Pointer jobAttributes, String name);

        boolean AssignProcessToJobObject(Pointer job, Pointer process);

        boolean QueryInformationJobObject(
            Pointer job,
            int infoClass,
            JnaJobObjectBasicLimitInformation info,
            int infoLength,
            Pointer returnLength
        );

        boolean SetInformationJobObject(Pointer job, int infoClass, JnaJobObjectBasicLimitInformation info, int infoLength);
    }

    private final NativeFunctions functions;
    private NativeHandlerCallback consoleCtrlHandlerCallback = null;

    JnaKernel32Library() {
        this.functions = Native.load("kernel32", NativeFunctions.class);
    }

    @Override
    public Handle GetCurrentProcess() {
        return new JnaHandle(functions.GetCurrentProcess());
    }

    @Override
    public boolean CloseHandle(Handle handle) {
        assert handle instanceof JnaHandle;
        var jnaHandle = (JnaHandle) handle;
        return functions.CloseHandle(jnaHandle.pointer);
    }

    @Override
    public int GetLastError() {
        // JNA does not like linking direclty to GetLastError, so we must use the Native helper function
        return Native.getLastError();
    }

    @Override
    public MemoryBasicInformation newMemoryBasicInformation() {
        return new JnaMemoryBasicInformation();
    }

    @Override
    public boolean VirtualLock(Address address, long size) {
        assert address instanceof JnaAddress;
        var jnaAddress = (JnaAddress) address;
        return functions.VirtualLock(jnaAddress.pointer, new SizeT(size));
    }

    @Override
    public int VirtualQueryEx(Handle handle, Address address, MemoryBasicInformation memoryInfo) {
        assert handle instanceof JnaHandle;
        assert address instanceof JnaAddress;
        assert memoryInfo instanceof JnaMemoryBasicInformation;
        var jnaHandle = (JnaHandle) handle;
        var jnaAddress = (JnaAddress) address;
        var jnaMemoryInfo = (JnaMemoryBasicInformation) memoryInfo;
        return functions.VirtualQueryEx(jnaHandle.pointer, jnaAddress.pointer, jnaMemoryInfo, jnaMemoryInfo.size());
    }

    @Override
    public boolean SetProcessWorkingSetSize(Handle handle, long minSize, long maxSize) {
        assert handle instanceof JnaHandle;
        var jnaHandle = (JnaHandle) handle;
        return functions.SetProcessWorkingSetSize(jnaHandle.pointer, new SizeT(minSize), new SizeT(maxSize));
    }

    @Override
    public int GetCompressedFileSizeW(String lpFileName, IntConsumer lpFileSizeHigh) {
        var wideFileName = new WString(lpFileName);
        var fileSizeHigh = new IntByReference();
        int ret = functions.GetCompressedFileSizeW(wideFileName, fileSizeHigh);
        lpFileSizeHigh.accept(fileSizeHigh.getValue());
        return ret;
    }

    @Override
    public int GetShortPathNameW(String lpszLongPath, char[] lpszShortPath, int cchBuffer) {
        var wideFileName = new WString(lpszLongPath);
        return functions.GetShortPathNameW(wideFileName, lpszShortPath, cchBuffer);
    }

    @Override
    public boolean SetConsoleCtrlHandler(ConsoleCtrlHandler handler, boolean add) {
        assert consoleCtrlHandlerCallback == null;
        consoleCtrlHandlerCallback = new NativeHandlerCallback(handler);
        return functions.SetConsoleCtrlHandler(consoleCtrlHandlerCallback, true);
    }

    @Override
    public Handle CreateJobObjectW() {
        return new JnaHandle(functions.CreateJobObjectW(null, null));
    }

    @Override
    public boolean AssignProcessToJobObject(Handle job, Handle process) {
        assert job instanceof JnaHandle;
        assert process instanceof JnaHandle;
        var jnaJob = (JnaHandle) job;
        var jnaProcess = (JnaHandle) process;
        return functions.AssignProcessToJobObject(jnaJob.pointer, jnaProcess.pointer);
    }

    @Override
    public JobObjectBasicLimitInformation newJobObjectBasicLimitInformation() {
        return new JnaJobObjectBasicLimitInformation();
    }

    @Override
    public boolean QueryInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        assert job instanceof JnaHandle;
        assert info instanceof JnaJobObjectBasicLimitInformation;
        var jnaJob = (JnaHandle) job;
        var jnaInfo = (JnaJobObjectBasicLimitInformation) info;
        var ret = functions.QueryInformationJobObject(jnaJob.pointer, infoClass, jnaInfo, jnaInfo.size(), null);
        return ret;
    }

    @Override
    public boolean SetInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        assert job instanceof JnaHandle;
        assert info instanceof JnaJobObjectBasicLimitInformation;
        var jnaJob = (JnaHandle) job;
        var jnaInfo = (JnaJobObjectBasicLimitInformation) info;
        return functions.SetInformationJobObject(jnaJob.pointer, infoClass, jnaInfo, jnaInfo.size());
    }
}
