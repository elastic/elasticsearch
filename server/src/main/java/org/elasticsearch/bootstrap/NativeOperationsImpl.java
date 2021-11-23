/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.WString;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

import static org.elasticsearch.bootstrap.JNAKernel32Library.SizeT;

/**
 * This class performs the actual work with JNA and library bindings to call native methods. It should only be used after
 * we are sure that the JNA classes are available to the JVM
 */
final class NativeOperationsImpl implements NativeOperations {

    private static final Logger logger = LogManager.getLogger(NativeOperationsImpl.class);

    private static final CLibrary CLIBRARY = getCLibraryOrNull();

    static CLibrary getCLibraryOrNull() {
        // prefer panama if available, fall back to JNA
        return PanamaCLibrary.instance().orElseGet(() -> JNACLibrary.instance().orElse(null));
    }

    private static final NativeOperationsImpl INSTANCE = instanceOrNull();

    private static NativeOperationsImpl instanceOrNull() {
        return CLIBRARY != null ? new NativeOperationsImpl() : null;
    }

    /** Returns the JNA instance or null. */
    static Optional<NativeOperations> getInstance() {
        return Optional.ofNullable(INSTANCE);
    }

    /** no instantiation */
    private NativeOperationsImpl() {}

    @Override
    public boolean tryLockMemory() {
        if (Constants.WINDOWS) {
            return tryVirtualLock();
        } else {
            return trySetMlockall();
        }
    }

    private static boolean trySetMlockall() {
        int errno = Integer.MIN_VALUE;
        String errMsg = null;
        boolean rlimitSuccess = false;
        long softLimit = 0;
        long hardLimit = 0;

        try {
            int result = CLIBRARY.mlockall(CLibrary.MCL_CURRENT);
            if (result == 0) {
                return true;
            }

            errno = CLIBRARY.getLastError();
            errMsg = CLIBRARY.strerror(errno);
            if (Constants.LINUX || Constants.MAC_OS_X) {
                // we only know RLIMIT_MEMLOCK for these two at the moment.
                try (CLibrary.Rlimit rlimit = CLIBRARY.newRlimit()) {
                    if (CLIBRARY.getrlimit(CLibrary.RLIMIT_MEMLOCK, rlimit) == 0) {
                        rlimitSuccess = true;
                        softLimit = rlimit.rlim_cur();
                        hardLimit = rlimit.rlim_max();
                    } else {
                        logger.warn("Unable to retrieve resource limits: {}", CLIBRARY.strerror(CLIBRARY.getLastError()));
                    }
                }
            }
        } catch (UnsatisfiedLinkError e) {
            // this will have already been logged by CLibrary, no need to repeat it
            // return;
        }

        // mlockall failed for some reason
        logger.warn("Unable to lock JVM Memory: error={}, reason={}", errno, errMsg);
        logger.warn("This can result in part of the JVM being swapped out.");
        if (errno == JNACLibrary.ENOMEM) {
            if (rlimitSuccess) {
                logger.warn(
                    "Increase RLIMIT_MEMLOCK, soft limit: {}, hard limit: {}",
                    rlimitToString(softLimit),
                    rlimitToString(hardLimit)
                );
                if (Constants.LINUX) {
                    // give specific instructions for the linux case to make it easy
                    String user = System.getProperty("user.name");
                    logger.warn(
                        "These can be adjusted by modifying /etc/security/limits.conf, for example: \n"
                            + "\t# allow user '{}' mlockall\n"
                            + "\t{} soft memlock unlimited\n"
                            + "\t{} hard memlock unlimited",
                        user,
                        user,
                        user
                    );
                    logger.warn("If you are logged in interactively, you will have to re-login for the new limits to take effect.");
                }
            } else {
                logger.warn("Increase RLIMIT_MEMLOCK (ulimit).");
            }
        }
        return false;
    }

    @Override
    public OptionalLong tryRetrieveMaxNumberOfThreads() {
        if (Constants.LINUX) {
            // this is only valid on Linux and the value *is* different on OS X
            // see /usr/include/sys/resource.h on OS X
            // on Linux the resource RLIMIT_NPROC means *the number of threads*
            // this is in opposition to BSD-derived OSes
            final int rlimit_nproc = 6;

            try (CLibrary.Rlimit rlimit = CLIBRARY.newRlimit()) {
                if (CLIBRARY.getrlimit(rlimit_nproc, rlimit) == 0) {
                    return OptionalLong.of(rlimit.rlim_cur());
                } else {
                    logger.warn("unable to retrieve max number of threads [" + CLIBRARY.strerror(CLIBRARY.getLastError()) + "]");
                }
            }
        }
        return OptionalLong.empty();
    }

    @Override
    public OptionalLong tryRetrieveMaxVirtualMemorySize() {
        if (Constants.LINUX || Constants.MAC_OS_X) {
            try (CLibrary.Rlimit rlimit = CLIBRARY.newRlimit()) {
                if (CLIBRARY.getrlimit(CLibrary.RLIMIT_AS, rlimit) == 0) {
                    return OptionalLong.of(rlimit.rlim_cur());
                } else {
                    logger.warn("unable to retrieve max size virtual memory [" + CLIBRARY.strerror(CLIBRARY.getLastError()) + "]");
                }
            }
        }
        return OptionalLong.empty();
    }

    @Override
    public OptionalLong tryRetrieveMaxFileSize() {
        if (Constants.LINUX || Constants.MAC_OS_X) {
            try (CLibrary.Rlimit rlimit = CLIBRARY.newRlimit()) {
                if (CLIBRARY.getrlimit(CLibrary.RLIMIT_FSIZE, rlimit) == 0) {
                    return OptionalLong.of(rlimit.rlim_cur());
                } else {
                    logger.warn("unable to retrieve max file size [" + CLIBRARY.strerror(CLIBRARY.getLastError()) + "]");
                }
            }
        }
        return OptionalLong.empty();
    }

    static String rlimitToString(long value) {
        assert Constants.LINUX || Constants.MAC_OS_X;
        if (value == CLibrary.RLIM_INFINITY) {
            return "unlimited";
        } else {
            return Long.toUnsignedString(value);
        }
    }

    /** Returns true if user is root, false if not, or if we don't know */
    @Override
    public boolean definitelyRunningAsRoot() {
        if (Constants.WINDOWS) {
            return false; // don't know
        }
        try {
            return CLIBRARY.geteuid() == 0;
        } catch (UnsatisfiedLinkError e) {
            // this will have already been logged by Kernel32Library, no need to repeat it // TODO: this this preexisting message
            return false;
        }
    }

    private boolean tryVirtualLock() {  // TODO: Panamaize
        JNAKernel32Library kernel = JNAKernel32Library.getInstance();
        Pointer process = null;
        try {
            process = kernel.GetCurrentProcess();
            // By default, Windows limits the number of pages that can be locked.
            // Thus, we need to first increase the working set size of the JVM by
            // the amount of memory we wish to lock, plus a small overhead (1MB).
            SizeT size = new SizeT(JvmInfo.jvmInfo().getMem().getHeapInit().getBytes() + (1024 * 1024));
            if (kernel.SetProcessWorkingSetSize(process, size, size) == false) {
                logger.warn("Unable to lock JVM memory. Failed to set working set size. Error code {}", CLIBRARY.getLastError());
            } else {
                JNAKernel32Library.MemoryBasicInformation memInfo = new JNAKernel32Library.MemoryBasicInformation();
                long address = 0;
                while (kernel.VirtualQueryEx(process, new Pointer(address), memInfo, memInfo.size()) != 0) {
                    boolean lockable = memInfo.State.longValue() == JNAKernel32Library.MEM_COMMIT
                        && (memInfo.Protect.longValue() & JNAKernel32Library.PAGE_NOACCESS) != JNAKernel32Library.PAGE_NOACCESS
                        && (memInfo.Protect.longValue() & JNAKernel32Library.PAGE_GUARD) != JNAKernel32Library.PAGE_GUARD;
                    if (lockable) {
                        kernel.VirtualLock(memInfo.BaseAddress, new SizeT(memInfo.RegionSize.longValue()));
                    }
                    // Move to the next region
                    address += memInfo.RegionSize.longValue();
                }
                return true;
            }
        } catch (UnsatisfiedLinkError e) {
            // this will have already been logged by Kernel32Library, no need to repeat it
        } finally {
            if (process != null) {
                kernel.CloseHandle(process);
            }
        }
        return false;
    }

    /**
     * Retrieves the short path form of the specified path.
     *
     * @param path the path
     * @return the short path name (or the original path if getting the short path name fails for any reason)
     */
    @Override
    public String getShortPathName(String path) { // TODO: Panamaize
        assert Constants.WINDOWS;
        try {
            final WString longPath = new WString("\\\\?\\" + path);
            // first we get the length of the buffer needed
            final int length = JNAKernel32Library.getInstance().GetShortPathNameW(longPath, null, 0);
            if (length == 0) {
                logger.warn("failed to get short path name: {}", CLIBRARY.getLastError());
                return path;
            }
            final char[] shortPath = new char[length];
            // knowing the length of the buffer, now we get the short name
            if (JNAKernel32Library.getInstance().GetShortPathNameW(longPath, shortPath, length) > 0) {
                return Native.toString(shortPath);
            } else {
                logger.warn("failed to get short path name: {}", CLIBRARY.getLastError());
                return path;
            }
        } catch (final UnsatisfiedLinkError e) {
            return path;
        }
    }

    @Override
    public void addConsoleCtrlHandler(ConsoleCtrlHandler handler) { // TODO: Panamaize
        // The console Ctrl handler is necessary on Windows platforms only.
        if (Constants.WINDOWS) {
            try {
                boolean result = JNAKernel32Library.getInstance().addConsoleCtrlHandler(handler);
                if (result) {
                    logger.debug("console ctrl handler correctly set");
                } else {
                    logger.warn("unknown error {} when adding console ctrl handler", CLIBRARY.getLastError());
                }
            } catch (UnsatisfiedLinkError e) {
                // this will have already been logged by Kernel32Library, no need to repeat it
            }
        }
    }

    @Override
    public int tryInstallSystemCallFilter(Path tmpFile) {
        try {
            return SystemCallFilter.init(tmpFile);
        } catch (Exception e) {
            // this is likely to happen unless the kernel is newish, its a best effort at the moment
            // so we log stacktrace at debug for now...
            if (logger.isDebugEnabled()) {
                logger.debug("unable to install syscall filter", e);
            }
            logger.warn("unable to install syscall filter: ", e);
        }
        return -1;  // uninstalled
    }
}
