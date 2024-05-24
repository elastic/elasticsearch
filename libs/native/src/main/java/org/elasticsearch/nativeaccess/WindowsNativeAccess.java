/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.Kernel32Library;
import org.elasticsearch.nativeaccess.lib.Kernel32Library.Handle;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;

import java.util.Optional;

import static java.lang.management.ManagementFactory.getMemoryMXBean;

class WindowsNativeAccess extends AbstractNativeAccess {

    /**
     * Memory protection constraints
     *
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa366786%28v=vs.85%29.aspx">docs</a>
     */
    public static final int PAGE_NOACCESS = 0x0001;
    public static final int PAGE_GUARD = 0x0100;
    public static final int MEM_COMMIT = 0x1000;

    private final Kernel32Library kernel;
    private final WindowsFunctions windowsFunctions;

    WindowsNativeAccess(NativeLibraryProvider libraryProvider) {
        super("Windows", libraryProvider);
        this.kernel = libraryProvider.getLibrary(Kernel32Library.class);
        this.windowsFunctions = new WindowsFunctions(kernel);
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        return false; // don't know
    }

    @Override
    public void tryLockMemory() {
        Handle process = kernel.GetCurrentProcess();
        // By default, Windows limits the number of pages that can be locked.
        // Thus, we need to first increase the working set size of the JVM by
        // the amount of memory we wish to lock, plus a small overhead (1MB).
        long size = getMemoryMXBean().getHeapMemoryUsage().getInit() + (1024 * 1024);
        if (kernel.SetProcessWorkingSetSize(process, size, size) == false) {
            logger.warn("Unable to lock JVM memory. Failed to set working set size. Error code {}", kernel.GetLastError());
        } else {
            var memInfo = kernel.newMemoryBasicInformation();
            var address = memInfo.BaseAddress();
            while (kernel.VirtualQueryEx(process, address, memInfo) != 0) {
                boolean lockable = memInfo.State() == MEM_COMMIT
                    && (memInfo.Protect() & PAGE_NOACCESS) != PAGE_NOACCESS
                    && (memInfo.Protect() & PAGE_GUARD) != PAGE_GUARD;
                if (lockable) {
                    kernel.VirtualLock(memInfo.BaseAddress(), memInfo.RegionSize());
                }
                // Move to the next region
                address = address.add(memInfo.RegionSize());
            }
            isMemoryLocked = true;
        }
        // note: no need to close the process handle because GetCurrentProcess returns a pseudo handle
    }

    @Override
    public ProcessLimits getProcessLimits() {
        return new ProcessLimits(ProcessLimits.UNKNOWN, ProcessLimits.UNKNOWN, ProcessLimits.UNKNOWN);
    }

    @Override
    public WindowsFunctions getWindowsFunctions() {
        return windowsFunctions;
    }

    @Override
    public Optional<VectorSimilarityFunctions> getVectorSimilarityFunctions() {
        return Optional.empty(); // not supported yet
    }
}
