/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.Kernel32Library;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;

import java.lang.management.ManagementFactory;

class WindowsNativeAccess extends AbstractNativeAccess {

    /**
     * Memory protection constraints
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/aa366786%28v=vs.85%29.aspx
     */
    public static final int PAGE_NOACCESS = 0x0001;
    public static final int PAGE_GUARD = 0x0100;
    public static final int MEM_COMMIT = 0x1000;

    private final Kernel32Library kernel;

    WindowsNativeAccess(NativeLibraryProvider libraryProvider) {
        kernel = libraryProvider.getLibrary(Kernel32Library.class);
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        return false; // don't know
    }

    @Override
    public void tryLockMemory() {
        long processHandle = 0;
        try {
            processHandle = kernel.GetCurrentProcess();
            // By default, Windows limits the number of pages that can be locked.
            // Thus, we need to first increase the working set size of the JVM by
            // the amount of memory we wish to lock, plus a small overhead (1MB).
            long size = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getInit() + (1024 * 1024);
            if (kernel.SetProcessWorkingSetSize(processHandle, size, size) == false) {
                logger.warn("Unable to lock JVM memory. Failed to set working set size. Error code {}", kernel.GetLastError());
            } else {
                var memInfo = kernel.newMemoryBasicInformation();
                long address = 0;
                while (kernel.VirtualQueryEx(processHandle, address, memInfo) != 0) {
                    boolean lockable = memInfo.getState() == MEM_COMMIT
                        && (memInfo.getProtect() & PAGE_NOACCESS) != PAGE_NOACCESS
                        && (memInfo.getProtect() & PAGE_GUARD) != PAGE_GUARD;
                    if (lockable) {
                        kernel.VirtualLock(memInfo.getBaseAddress(), memInfo.getRegionSize());
                    }
                    // Move to the next region
                    address += memInfo.getRegionSize();
                }
                memoryLocked = true;
            }
        } finally {
            if (processHandle != 0) {
                kernel.CloseHandle(processHandle);
            }
        }
    }

    @Override
    public void trySetMaxNumberOfThreads() {
        // no way to set limit for number of threads in Windows
    }

    @Override
    public void trySetMaxVirtualMemorySize() {
        // no way to set limit for virtual memory size in Windows
    }

    @Override
    public void trySetMaxFileSize() {
        // no way to set limit for max file size in Windows
    }
}
