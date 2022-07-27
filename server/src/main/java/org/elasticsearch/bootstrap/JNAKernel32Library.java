/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.sun.jna.IntegerType;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.WString;
import com.sun.jna.win32.StdCallLibrary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Library for Windows/Kernel32
 */
final class JNAKernel32Library {

    private static final Logger logger = LogManager.getLogger(JNAKernel32Library.class);

    // Callbacks must be kept around in order to be able to be called later,
    // when the Windows ConsoleCtrlHandler sends an event.
    private List<NativeHandlerCallback> callbacks = new ArrayList<>();

    // Native library instance must be kept around for the same reason.
    private static final class Holder {
        private static final JNAKernel32Library instance = new JNAKernel32Library();
    }

    private JNAKernel32Library() {
        if (Constants.WINDOWS) {
            try {
                Native.register("kernel32");
                logger.debug("windows/Kernel32 library loaded");
            } catch (NoClassDefFoundError e) {
                logger.warn("JNA not found. native methods and handlers will be disabled.");
            } catch (UnsatisfiedLinkError e) {
                logger.warn("unable to link Windows/Kernel32 library. native methods and handlers will be disabled.");
            }
        }
    }

    static JNAKernel32Library getInstance() {
        return Holder.instance;
    }

    /**
     * Adds a Console Ctrl Handler.
     *
     * @return true if the handler is correctly set
     * @throws java.lang.UnsatisfiedLinkError if the Kernel32 library is not loaded or if the native function is not found
     * @throws java.lang.NoClassDefFoundError if the library for native calls is missing
     */
    boolean addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        boolean result = false;
        if (handler != null) {
            NativeHandlerCallback callback = new NativeHandlerCallback(handler);
            result = SetConsoleCtrlHandler(callback, true);
            if (result) {
                callbacks.add(callback);
            }
        }
        return result;
    }

    List<Object> getCallbacks() {
        return Collections.<Object>unmodifiableList(callbacks);
    }

    /**
     * Native call to the Kernel32 API to set a new Console Ctrl Handler.
     *
     * @return true if the handler is correctly set
     * @throws java.lang.UnsatisfiedLinkError if the Kernel32 library is not loaded or if the native function is not found
     * @throws java.lang.NoClassDefFoundError if the library for native calls is missing
     */
    native boolean SetConsoleCtrlHandler(StdCallLibrary.StdCallCallback handler, boolean add);

    /**
     * Handles consoles event with WIN API
     * <p>
     * See http://msdn.microsoft.com/en-us/library/windows/desktop/ms683242%28v=vs.85%29.aspx
     */
    class NativeHandlerCallback implements StdCallLibrary.StdCallCallback {

        private final ConsoleCtrlHandler handler;

        NativeHandlerCallback(ConsoleCtrlHandler handler) {
            this.handler = handler;
        }

        public boolean callback(long dwCtrlType) {
            int event = (int) dwCtrlType;
            if (logger.isDebugEnabled()) {
                logger.debug("console control handler receives event [{}@{}]", event, dwCtrlType);

            }
            return handler.handle(event);
        }
    }

    /**
     * Memory protection constraints
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/aa366786%28v=vs.85%29.aspx
     */
    public static final int PAGE_NOACCESS = 0x0001;
    public static final int PAGE_GUARD = 0x0100;
    public static final int MEM_COMMIT = 0x1000;

    /**
     * Contains information about a range of pages in the virtual address space of a process.
     * The VirtualQuery and VirtualQueryEx functions use this structure.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/aa366775%28v=vs.85%29.aspx
     */
    public static class MemoryBasicInformation extends Structure {
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
    }

    public static class SizeT extends IntegerType {

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
     * Locks the specified region of the process's virtual address space into physical
     * memory, ensuring that subsequent access to the region will not incur a page fault.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/aa366895%28v=vs.85%29.aspx
     *
     * @param address A pointer to the base address of the region of pages to be locked.
     * @param size The size of the region to be locked, in bytes.
     * @return true if the function succeeds
     */
    native boolean VirtualLock(Pointer address, SizeT size);

    /**
     * Retrieves information about a range of pages within the virtual address space of a specified process.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/aa366907%28v=vs.85%29.aspx
     *
     * @param handle A handle to the process whose memory information is queried.
     * @param address A pointer to the base address of the region of pages to be queried.
     * @param memoryInfo A pointer to a structure in which information about the specified page range is returned.
     * @param length The size of the buffer pointed to by the memoryInfo parameter, in bytes.
     * @return the actual number of bytes returned in the information buffer.
     */
    native int VirtualQueryEx(Pointer handle, Pointer address, MemoryBasicInformation memoryInfo, int length);

    /**
     * Sets the minimum and maximum working set sizes for the specified process.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms686234%28v=vs.85%29.aspx
     *
     * @param handle A handle to the process whose working set sizes is to be set.
     * @param minSize The minimum working set size for the process, in bytes.
     * @param maxSize The maximum working set size for the process, in bytes.
     * @return true if the function succeeds.
     */
    native boolean SetProcessWorkingSetSize(Pointer handle, SizeT minSize, SizeT maxSize);

    /**
     * Retrieves a pseudo handle for the current process.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms683179%28v=vs.85%29.aspx
     *
     * @return a pseudo handle to the current process.
     */
    native Pointer GetCurrentProcess();

    /**
     * Closes an open object handle.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms724211%28v=vs.85%29.aspx
     *
     * @param handle A valid handle to an open object.
     * @return true if the function succeeds.
     */
    native boolean CloseHandle(Pointer handle);

    /**
     * Retrieves the short path form of the specified path. See
     * <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa364989.aspx">{@code GetShortPathName}</a>.
     *
     * @param lpszLongPath  the path string
     * @param lpszShortPath a buffer to receive the short name
     * @param cchBuffer     the size of the buffer
     * @return the length of the string copied into {@code lpszShortPath}, otherwise zero for failure
     */
    native int GetShortPathNameW(WString lpszLongPath, char[] lpszShortPath, int cchBuffer);

    /**
     * Creates or opens a new job object
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms682409%28v=vs.85%29.aspx
     *
     * @param jobAttributes security attributes
     * @param name job name
     * @return job handle if the function succeeds
     */
    native Pointer CreateJobObjectW(Pointer jobAttributes, String name);

    /**
     * Associates a process with an existing job
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms681949%28v=vs.85%29.aspx
     *
     * @param job job handle
     * @param process process handle
     * @return true if the function succeeds
     */
    native boolean AssignProcessToJobObject(Pointer job, Pointer process);

    /**
     * Basic limit information for a job object
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms684147%28v=vs.85%29.aspx
     */
    public static class JOBOBJECT_BASIC_LIMIT_INFORMATION extends Structure implements Structure.ByReference {
        public long PerProcessUserTimeLimit;
        public long PerJobUserTimeLimit;
        public int LimitFlags;
        public SizeT MinimumWorkingSetSize;
        public SizeT MaximumWorkingSetSize;
        public int ActiveProcessLimit;
        public Pointer Affinity;
        public int PriorityClass;
        public int SchedulingClass;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(
                "PerProcessUserTimeLimit",
                "PerJobUserTimeLimit",
                "LimitFlags",
                "MinimumWorkingSetSize",
                "MaximumWorkingSetSize",
                "ActiveProcessLimit",
                "Affinity",
                "PriorityClass",
                "SchedulingClass"
            );
        }
    }

    /**
     * Constant for JOBOBJECT_BASIC_LIMIT_INFORMATION in Query/Set InformationJobObject
     */
    static final int JOBOBJECT_BASIC_LIMIT_INFORMATION_CLASS = 2;

    /**
     * Constant for LimitFlags, indicating a process limit has been set
     */
    static final int JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 8;

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
    native boolean QueryInformationJobObject(Pointer job, int infoClass, Pointer info, int infoLength, Pointer returnLength);

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
    native boolean SetInformationJobObject(Pointer job, int infoClass, Pointer info, int infoLength);
}
