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

    /**
     * Constant for JOBOBJECT_BASIC_LIMIT_INFORMATION in Query/Set InformationJobObject
     */
    private static final int JOBOBJECT_BASIC_LIMIT_INFORMATION_CLASS = 2;

    /**
     * Constant for LimitFlags, indicating a process limit has been set
     */
    private static final int JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 8;

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

    /**
     * Install exec system call filtering on Windows.
     * <p>
     * Process creation is restricted with {@code SetInformationJobObject/ActiveProcessLimit}.
     * <p>
     * Note: This is not intended as a real sandbox. It is another level of security, mostly intended to annoy
     * security researchers and make their lives more difficult in achieving "remote execution" exploits.
     */
    @Override
    public void tryInstallExecSandbox() {
        // create a new Job
        Handle job = kernel.CreateJobObjectW();
        if (job == null) {
            throw new UnsupportedOperationException("CreateJobObject: " + kernel.GetLastError());
        }

        try {
            // retrieve the current basic limits of the job
            int clazz = JOBOBJECT_BASIC_LIMIT_INFORMATION_CLASS;
            var info = kernel.newJobObjectBasicLimitInformation();
            if (kernel.QueryInformationJobObject(job, clazz, info) == false) {
                throw new UnsupportedOperationException("QueryInformationJobObject: " + kernel.GetLastError());
            }
            // modify the number of active processes to be 1 (exactly the one process we will add to the job).
            info.setActiveProcessLimit(1);
            info.setLimitFlags(JOB_OBJECT_LIMIT_ACTIVE_PROCESS);
            if (kernel.SetInformationJobObject(job, clazz, info) == false) {
                throw new UnsupportedOperationException("SetInformationJobObject: " + kernel.GetLastError());
            }
            // assign ourselves to the job
            if (kernel.AssignProcessToJobObject(job, kernel.GetCurrentProcess()) == false) {
                throw new UnsupportedOperationException("AssignProcessToJobObject: " + kernel.GetLastError());
            }
        } finally {
            kernel.CloseHandle(job);
        }

        execSandboxState = ExecSandboxState.ALL_THREADS;
        logger.debug("Windows ActiveProcessLimit initialization successful");
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
