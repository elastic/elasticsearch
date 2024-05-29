/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.nativeaccess.WindowsFunctions.ConsoleCtrlHandler;

public non-sealed interface Kernel32Library extends NativeLibrary {
    interface Handle {}

    interface Address {
        Address add(long offset);
    }

    Handle GetCurrentProcess();

    boolean CloseHandle(Handle handle);

    int GetLastError();

    /**
     * Contains information about a range of pages in the virtual address space of a process.
     * The VirtualQuery and VirtualQueryEx functions use this structure.
     *
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa366775%28v=vs.85%29.aspx">MemoryBasicInformation docs</a>
     */
    interface MemoryBasicInformation {
        Address BaseAddress();

        long RegionSize();

        long State();

        long Protect();

        long Type();
    }

    /**
     * Create a new MemoryBasicInformation for use by VirtualQuery and VirtualQueryEx
     */
    MemoryBasicInformation newMemoryBasicInformation();

    /**
     * Locks the specified region of the process's virtual address space into physical
     * memory, ensuring that subsequent access to the region will not incur a page fault.
     *
     * @param address A pointer to the base address of the region of pages to be locked.
     * @param size The size of the region to be locked, in bytes.
     * @return true if the function succeeds
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa366895%28v=vs.85%29.aspx">VirtualLock docs</a>
     */
    boolean VirtualLock(Address address, long size);

    /**
     * Retrieves information about a range of pages within the virtual address space of a specified process.
     *
     * Note: the dwLength parameter is handled by the underlying implementation
     *
     * @param handle A handle to the process whose memory information is queried.
     * @param address A pointer to the base address of the region of pages to be queried.
     * @param memoryInfo A pointer to a structure in which information about the specified page range is returned.
     * @return the actual number of bytes returned in the information buffer.
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa366907%28v=vs.85%29.aspx">VirtualQueryEx docs</a>
     */
    int VirtualQueryEx(Handle handle, Address address, MemoryBasicInformation memoryInfo);

    /**
     * Sets the minimum and maximum working set sizes for the specified process.
     *
     * @param handle A handle to the process whose working set sizes is to be set.
     * @param minSize The minimum working set size for the process, in bytes.
     * @param maxSize The maximum working set size for the process, in bytes.
     * @return true if the function succeeds.
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/ms686234%28v=vs.85%29.aspx">SetProcessWorkingSetSize docs</a>
     */
    boolean SetProcessWorkingSetSize(Handle handle, long minSize, long maxSize);

    /**
     * Retrieves the short path form of the specified path.
     *
     * @param lpszLongPath  the path string
     * @param lpszShortPath a buffer to receive the short name
     * @param cchBuffer     the size of the buffer
     * @return the length of the string copied into {@code lpszShortPath}, otherwise zero for failure
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa364989.aspx">GetShortPathName docs</a>
     */
    int GetShortPathNameW(String lpszLongPath, char[] lpszShortPath, int cchBuffer);

    /**
     * Native call to the Kernel32 API to set a new Console Ctrl Handler.
     *
     * @param handler A callback to handle control events
     * @param add     True if the handler should be added, false if it should replace existing handlers
     * @return true if the handler is correctly set
     * @see <a href="https://learn.microsoft.com/en-us/windows/console/setconsolectrlhandler">SetConsoleCtrlHandler docs</a>
     */
    boolean SetConsoleCtrlHandler(ConsoleCtrlHandler handler, boolean add);
}
