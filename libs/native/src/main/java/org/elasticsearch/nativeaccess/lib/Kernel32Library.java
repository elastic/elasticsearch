/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.NativeAccess.ConsoleCtrlHandler;

import java.util.function.IntConsumer;

/**
 * Windows kernel methods.
 */
public interface Kernel32Library {

    long GetCurrentProcess();

    boolean CloseHandle(long handle);

    int GetLastError();

    /**
     * Contains information about a range of pages in the virtual address space of a process.
     * The VirtualQuery and VirtualQueryEx functions use this structure.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/aa366775%28v=vs.85%29.aspx
     */
    interface MemoryBasicInformation {
        long BaseAddress();
        long AllocationBase();
        long AllocationProtect();
        long RegionSize();
        long State();
        long Protect();
        long Type();
    }

    MemoryBasicInformation newMemoryBasicInformation();

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
    boolean VirtualLock(long address, long size);

    /**
     * Retrieves information about a range of pages within the virtual address space of a specified process.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/aa366907%28v=vs.85%29.aspx
     *
     * @param processHandle A handle to the process whose memory information is queried.
     * @param address A pointer to the base address of the region of pages to be queried.
     * @param memoryInfo A pointer to a structure in which information about the specified page range is returned.
     * @return the actual number of bytes returned in the information buffer.
     * @apiNote the dwLength parameter is handled by the underlying implementation
     */
    int VirtualQueryEx(long processHandle, long address, MemoryBasicInformation memoryInfo);

    /**
     * Sets the minimum and maximum working set sizes for the specified process.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms686234%28v=vs.85%29.aspx
     *
     * @param processHandle A handle to the process whose working set sizes is to be set.
     * @param minSize The minimum working set size for the process, in bytes.
     * @param maxSize The maximum working set size for the process, in bytes.
     * @return true if the function succeeds.
     */
    boolean SetProcessWorkingSetSize(long processHandle, long minSize, long maxSize);

    /**
     * Retrieves the actual number of bytes of disk storage used to store a specified file.
     *
     * https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getcompressedfilesizew
     *
     * @param lpFileName the path string
     * @param lpFileSizeHigh pointer to high-order DWORD for compressed file size (or null if not needed)
     * @return the low-order DWORD for compressed file size
     */
    int GetCompressedFileSizeW(String lpFileName, IntConsumer lpFileSizeHigh);

    /**
     * Retrieves the short path form of the specified path. See
     * <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa364989.aspx">{@code GetShortPathName}</a>.
     *
     * @param lpszLongPath  the path string
     * @param lpszShortPath a buffer to receive the short name
     * @param cchBuffer     the size of the buffer
     * @return the length of the string copied into {@code lpszShortPath}, otherwise zero for failure
     */
    int GetShortPathNameW(String lpszLongPath, char[] lpszShortPath, int cchBuffer);

    /**
     * Native call to the Kernel32 API to set a new Console Ctrl Handler.
     * <a href="https://learn.microsoft.com/en-us/windows/console/setconsolectrlhandler">{@code SetConsoleCtrlHandler}</a>
     *
     * @param handler A callback to handle control events
     * @param add     True if the handler should be added, false if it should replace existing handlers
     * @return true if the handler is correctly set
     */
    boolean SetConsoleCtrlHandler(ConsoleCtrlHandler handler, boolean add);
}
