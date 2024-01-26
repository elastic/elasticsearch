/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

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
        long getBaseAddress();
        long getAllocationBase();
        long getAllocationProtect();
        long getRegionSize();
        long getState();
        long getProtect();
        long getType();
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
}
