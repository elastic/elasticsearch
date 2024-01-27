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
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.NativeAccess.ConsoleCtrlHandler;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntConsumer;

class JnaStaticKernel32Library {
    private static final Logger logger = LogManager.getLogger(JnaStaticPosixCLibrary.class);

    static {
        Native.register("kernel32");
    }

    static class SizeT extends IntegerType {
        // JNA requires this no-arg constructor to be public,
        // otherwise it fails to register kernel32 library
        public SizeT() {
            this(0);
        }

        SizeT(long value) {
            super(Native.SIZE_T_SIZE, value);
        }
    }

    /**
     * @see org.elasticsearch.nativeaccess.lib.Kernel32Library.MemoryBasicInformation
     */
    static class JnaMemoryBasicInformation extends Structure implements Kernel32Library.MemoryBasicInformation {
        // note: these members must be public for jna to set them
        public Pointer BaseAddress;
        public Pointer AllocationBase;
        public NativeLong AllocationProtect;
        public SizeT RegionSize;
        public NativeLong State;
        public NativeLong Protect;
        public NativeLong Type;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("BaseAddress", "AllocationBase", "AllocationProtect", "RegionSize", "State", "Protect", "Type");
        }

        @Override
        public long BaseAddress() {
            return Pointer.nativeValue(BaseAddress);
        }

        @Override
        public long AllocationBase() {
            return Pointer.nativeValue(AllocationBase);
        }

        @Override
        public long AllocationProtect() {
            return AllocationProtect.longValue();
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
     * @see Kernel32Library#CloseHandle(long)
     */
    static native boolean CloseHandle(Pointer handle);

    /**
     * @see Kernel32Library#GetLastError()
     */
    static native int GetLastError();

    /**
     * @see org.elasticsearch.nativeaccess.lib.Kernel32Library#VirtualLock(long, long)
     */
    static native boolean VirtualLock(Pointer address, SizeT size);

    /**
     * @see org.elasticsearch.nativeaccess.lib.Kernel32Library#VirtualQueryEx(long, long, Kernel32Library.MemoryBasicInformation)
     */
    static native int VirtualQueryEx(Pointer handle, Pointer address, JnaMemoryBasicInformation memoryInfo, int length);

    /**
     * @see Kernel32Library#SetProcessWorkingSetSize(long, long, long)
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
}
