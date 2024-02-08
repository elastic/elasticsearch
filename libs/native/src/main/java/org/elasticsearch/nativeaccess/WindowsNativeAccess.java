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

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

class WindowsNativeAccess extends AbstractNativeAccess {

    /**
     * Memory protection constraints
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/aa366786%28v=vs.85%29.aspx
     */
    public static final int PAGE_NOACCESS = 0x0001;
    public static final int PAGE_GUARD = 0x0100;
    public static final int MEM_COMMIT = 0x1000;

    private static final int INVALID_FILE_SIZE = -1;

    /**
     * Constant for JOBOBJECT_BASIC_LIMIT_INFORMATION in Query/Set InformationJobObject
     */
    private static final int JOBOBJECT_BASIC_LIMIT_INFORMATION_CLASS = 2;

    /**
     * Constant for LimitFlags, indicating a process limit has been set
     */
    private static final int JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 8;

    private final Kernel32Library kernel;

    WindowsNativeAccess(NativeLibraryProvider libraryProvider) {
        super("Windows");
        kernel = libraryProvider.getLibrary(Kernel32Library.class);
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
        long size = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getInit() + (1024 * 1024);
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
            memoryLocked = true;
        }
        // note: no need to close the process handle because GetCurrentProcess returns a pseudo handle
    }

    @Override
    public void tryInitMaxNumberOfThreads() {
        // no way to set limit for number of threads in Windows
    }

    @Override
    public void tryInitMaxVirtualMemorySize() {
        // no way to set limit for virtual memory size in Windows
    }

    @Override
    public void tryInitMaxFileSize() {
        // no way to set limit for max file size in Windows
    }

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

        logger.debug("Windows ActiveProcessLimit initialization successful");
    }

    @Override
    public OptionalLong allocatedSizeInBytes(Path path) {
        assert Files.isRegularFile(path) : path;
        String fileName = "\\\\?\\" + path;
        AtomicInteger lpFileSizeHigh = new AtomicInteger();

        final int lpFileSizeLow = kernel.GetCompressedFileSizeW(fileName, lpFileSizeHigh::set);
        if (lpFileSizeLow == INVALID_FILE_SIZE) {
            logger.warn("Unable to get allocated size of file [{}]. Error code {}", path, kernel.GetLastError());
            return OptionalLong.empty();
        }

        // convert lpFileSizeLow to unsigned long and combine with signed/shifted lpFileSizeHigh
        final long allocatedSize = (((long) lpFileSizeHigh.get()) << Integer.SIZE) | Integer.toUnsignedLong(lpFileSizeLow);
        if (logger.isTraceEnabled()) {
            logger.trace(
                "executing native method GetCompressedFileSizeW returned [high={}, low={}, allocated={}] for file [{}]",
                lpFileSizeHigh.get(),
                lpFileSizeLow,
                allocatedSize,
                path
            );
        }
        return OptionalLong.of(allocatedSize);
    }

    @Override
    public String getShortPathName(String path) {
        String longPath = "\\\\?\\" + path;
        // first we get the length of the buffer needed
        final int length = kernel.GetShortPathNameW(longPath, null, 0);
        if (length == 0) {
            logger.warn("failed to get short path name: {}", kernel.GetLastError());
            return path;
        }
        final char[] shortPath = new char[length];
        // knowing the length of the buffer, now we get the short name
        if (kernel.GetShortPathNameW(longPath, shortPath, length) > 0) {
            assert shortPath[length - 1] == '\0';
            return new String(shortPath, 0, length - 1);
        } else {
            logger.warn("failed to get short path name: {}", kernel.GetLastError());
            return path;
        }
    }

    @Override
    public boolean addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        return kernel.SetConsoleCtrlHandler(dwCtrlType -> {
            if (logger.isDebugEnabled()) {
                logger.debug("console control handler received event [{}]", dwCtrlType);
            }
            return handler.handle(dwCtrlType);
        }, true);
    }

    @Override
    public void tryPreallocate(Path file, long size) {
        logger.warn("Cannot preallocate file size because operation is not available on Windows");
    }

    @Override
    public long getRlimitInfinity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int sd_notify(int unset_environment, String state) {
        throw new UnsupportedOperationException();
    }
}
