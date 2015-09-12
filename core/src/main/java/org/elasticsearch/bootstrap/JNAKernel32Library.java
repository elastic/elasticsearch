/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import com.sun.jna.*;
import com.sun.jna.win32.StdCallLibrary;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Library for Windows/Kernel32
 */
final class JNAKernel32Library {

    private static final ESLogger logger = Loggers.getLogger(JNAKernel32Library.class);

    // Callbacks must be kept around in order to be able to be called later,
    // when the Windows ConsoleCtrlHandler sends an event.
    private List<NativeHandlerCallback> callbacks = new ArrayList<>();

    // Native library instance must be kept around for the same reason.
    private final static class Holder {
        private final static JNAKernel32Library instance = new JNAKernel32Library();
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
     * @param handler
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
     * @param handler
     * @param add
     * @return true if the handler is correctly set
     * @throws java.lang.UnsatisfiedLinkError if the Kernel32 library is not loaded or if the native function is not found
     * @throws java.lang.NoClassDefFoundError if the library for native calls is missing
     */
    native boolean SetConsoleCtrlHandler(StdCallLibrary.StdCallCallback handler, boolean add);

    /**
     * Handles consoles event with WIN API
     * <p/>
     * See http://msdn.microsoft.com/en-us/library/windows/desktop/ms683242%28v=vs.85%29.aspx
     */
    class NativeHandlerCallback implements StdCallLibrary.StdCallCallback {

        private final ConsoleCtrlHandler handler;

        public NativeHandlerCallback(ConsoleCtrlHandler handler) {
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
            return Arrays.asList(new String[]{"BaseAddress", "AllocationBase", "AllocationProtect", "RegionSize", "State", "Protect", "Type"});
        }
    }

    public static class SizeT extends IntegerType {

        public SizeT() {
            this(0);
        }

        public SizeT(long value) {
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
}
