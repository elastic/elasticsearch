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
import com.sun.jna.WString;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.win32.StdCallLibrary;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.NativeAccess.ConsoleCtrlHandler;
import org.elasticsearch.nativeaccess.jna.JnaKernel32Library.JnaAddress;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;
import org.elasticsearch.nativeaccess.lib.Kernel32Library.Address;
import org.elasticsearch.nativeaccess.lib.Kernel32Library.MemoryBasicInformation;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntConsumer;

class JnaStaticKernel32Library {
    private static final Logger logger = LogManager.getLogger(JnaStaticPosixCLibrary.class);

    static {
        Native.register("kernel32");
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
     * @see Kernel32Library.MemoryBasicInformation
     */
    static class JnaMemoryBasicInformation extends Structure implements Kernel32Library.MemoryBasicInformation {
        // note: these members must be public for jna to set them
        public Pointer BaseAddress = new Pointer(0);
        public byte[] _ignore = new byte[16];
        public SizeT RegionSize = new SizeT();
        public NativeLong State;
        public NativeLong Protect;
        public NativeLong Type;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("BaseAddress", "_ignore", "RegionSize", "State", "Protect", "Type");
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
     * Handles consoles event with WIN API
     * <p>
     * See http://msdn.microsoft.com/en-us/library/windows/desktop/ms683242%28v=vs.85%29.aspx
     */
    static class NativeHandlerCallback implements StdCallLibrary.StdCallCallback {

        private final ConsoleCtrlHandler handler;

        NativeHandlerCallback(ConsoleCtrlHandler handler) {
            this.handler = handler;
        }

        public boolean callback(long dwCtrlType) {
            return handler.handle((int) dwCtrlType);
        }
    }

    /**
     * @see Kernel32Library#GetCurrentProcess()
     */
    static native Pointer GetCurrentProcess();

    /**
     * @see Kernel32Library#CloseHandle(Kernel32Library.Handle)
     */
    static native boolean CloseHandle(Pointer handle);

    /**
     * @see Kernel32Library#VirtualLock(Address, long)
     */
    static native boolean VirtualLock(Pointer address, SizeT size);

    /**
     * @see Kernel32Library#VirtualQueryEx(Kernel32Library.Handle, Address, MemoryBasicInformation)
     */
    static native int VirtualQueryEx(Pointer handle, Pointer address, JnaMemoryBasicInformation memoryInfo, int length);

    /**
     * @see Kernel32Library#SetProcessWorkingSetSize(Kernel32Library.Handle, long, long)
     */
    static native boolean SetProcessWorkingSetSize(Pointer handle, SizeT minSize, SizeT maxSize);

    /**
     * @see Kernel32Library#GetCompressedFileSizeW(String, IntConsumer)
     */
    static native int GetCompressedFileSizeW(WString lpFileName, IntByReference lpFileSizeHigh);

    /**
     * @see Kernel32Library#GetShortPathNameW(String, char[], int)
     */
    static native int GetShortPathNameW(WString lpszLongPath, char[] lpszShortPath, int cchBuffer);

    /**
     * Native call to the Kernel32 API to set a new Console Ctrl Handler.
     *
     * @return true if the handler is correctly set
     */
    static native boolean SetConsoleCtrlHandler(StdCallLibrary.StdCallCallback handler, boolean add);

    /**
     * Creates or opens a new job object
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms682409%28v=vs.85%29.aspx
     *
     * @param jobAttributes security attributes
     * @param name job name
     * @return job handle if the function succeeds
     */
    static native Pointer CreateJobObjectW(Pointer jobAttributes, String name);

    /**
     * Associates a process with an existing job
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms681949%28v=vs.85%29.aspx
     *
     * @param job job handle
     * @param process process handle
     * @return true if the function succeeds
     */
    static native boolean AssignProcessToJobObject(Pointer job, Pointer process);

    /**
     * Basic limit information for a job object
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms684147%28v=vs.85%29.aspx
     */
    static class JnaJobObjectBasicLimitInformation extends Structure
        implements
            Structure.ByReference,
            Kernel32Library.JobObjectBasicLimitInformation {
        public byte[] _ignore1 = new byte[16];
        public int LimitFlags;
        public byte[] _ignore2 = new byte[20];
        public int ActiveProcessLimit;
        public byte[] _ignore3 = new byte[20];

        JnaJobObjectBasicLimitInformation() {
            super(8);
        }

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("_ignore1", "LimitFlags", "_ignore2", "ActiveProcessLimit", "_ignore3");
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
     * Get job limit and state information
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms684925%28v=vs.85%29.aspx
     *
     * @param job job handle
     * @param infoClass information class constant
     * @param info pointer to information structure
     * @param infoLength size of information structure
     * @param returnLength length of data written back to structure (or null if not wanted)
     * @return true if the function succeeds
     */
    static native boolean QueryInformationJobObject(
        Pointer job,
        int infoClass,
        JnaJobObjectBasicLimitInformation info,
        int infoLength,
        Pointer returnLength
    );

    /**
     * Set job limit and state information
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms686216%28v=vs.85%29.aspx
     *
     * @param job job handle
     * @param infoClass information class constant
     * @param info pointer to information structure
     * @param infoLength size of information structure
     * @return true if the function succeeds
     */
    static native boolean SetInformationJobObject(Pointer job, int infoClass, JnaJobObjectBasicLimitInformation info, int infoLength);
}
