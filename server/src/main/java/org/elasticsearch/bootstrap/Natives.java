/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.OptionalLong;

/**
 * The Natives class is a wrapper class that checks if the classes necessary for calling native methods are available on
 * startup. If they are not available, this class will avoid calling code that loads these classes.
 */
final class Natives {

    static final NativeOperations nativeOperations = nativeOperations();

    private static NativeOperations nativeOperations() {
        return NativeOperationsImpl.getInstance().orElseGet(UnsupportedNativeOperationsImpl::new);
    }

    /** no instantiation */
    private Natives() {}

    private static final Logger logger = LogManager.getLogger(Natives.class);

    // marker to determine if either of Panama or JNA class files are available to the JVM
    static final boolean NATIVES_AVAILABLE = (nativeOperations instanceof UnsupportedNativeOperationsImpl) == false;

    private static boolean isMemoryLocked; // false

    /** Tries to lock the virtual memory.
     *
     * <p> This method always returns successfully, regardless of whether the virtual memory is locked or not. If the actual virtual memory
     * has been locked, then subsequent invocations of {@link #isMemoryLocked} return true. Otherwise, {@code isMemoryLocked} returns false.
     */
    static void tryLockMemory() {
        isMemoryLocked = nativeOperations.tryLockMemory();
    }

    static boolean isMemoryLocked() {
        return isMemoryLocked;
    }

    // --

    static boolean definitelyRunningAsRoot() {
        return nativeOperations.definitelyRunningAsRoot();
    }

    /**
     * Retrieves the short path form of the specified path.
     *
     * @param path the path
     * @return the short path name (or the original path if getting the short path name fails for any reason)
     */
    static String getShortPathName(final String path) {
        return nativeOperations.getShortPathName(path);
    }

    static void addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        nativeOperations.addConsoleCtrlHandler(handler);
    }

    // set to the maximum number of threads that can be created for
    // the user ID that owns the running Elasticsearch process
    private static OptionalLong maxNumberOfThreads = OptionalLong.empty();

    static void tryRetrieveMaxNumberOfThreads() {
        maxNumberOfThreads = nativeOperations.tryRetrieveMaxNumberOfThreads();
    }

    static OptionalLong maxNumberOfThreads() {
        return maxNumberOfThreads;
    }

    private static OptionalLong maxVirtualMemorySize = OptionalLong.empty(); // empty unless explicitly retrieved from the system

    /**
     * Tries to retrieve the max virtual memory size.
     *
     * <p> This method always returns successfully, regardless of whether the actual max virtual memory size has been retrieved or not.
     * If the actual max virtual memory size has been retrieved, then subsequent invocations of {@link #maxVirtualMemorySize} return a
     * non-empty optional with the retrieved value. Otherwise, {@code maxVirtualMemorySize} returns an empty optional.
     */
    static void tryRetrieveMaxVirtualMemorySize() {
        maxVirtualMemorySize = nativeOperations.tryRetrieveMaxVirtualMemorySize();
    }

    /**
     * Returns an optional with the max virtual memory size, if known. Otherwise, returns an empty optional if unknown or if invoked
     * prior to {@link #tryRetrieveMaxVirtualMemorySize}.
     */
    static OptionalLong maxVirtualMemorySize() {
        return maxVirtualMemorySize;
    }

    private static OptionalLong maxFileSize = OptionalLong.empty();

    static void tryRetrieveMaxFileSize() {
        maxFileSize = nativeOperations.tryRetrieveMaxFileSize();
    }

    static OptionalLong maxFileSize() {
        return maxFileSize;
    }

    private static final int LOCAL_SYSTEM_CALL_FILTER_UNINSTALLED = -1;
    private static final int LOCAL_SYSTEM_CALL_FILTER = 0;
    private static final int LOCAL_SYSTEM_CALL_FILTER_ALL = 1;
    private static int systemCallFilterInstalledMask = LOCAL_SYSTEM_CALL_FILTER_UNINSTALLED;

    static void tryInstallSystemCallFilter(Path tmpFile) {
        systemCallFilterInstalledMask = nativeOperations.tryInstallSystemCallFilter(tmpFile);
    }

    static boolean isSystemCallFilterInstalled() {
        return systemCallFilterInstalledMask == LOCAL_SYSTEM_CALL_FILTER || systemCallFilterInstalledMask == LOCAL_SYSTEM_CALL_FILTER_ALL;
    }

    static boolean isAllSystemCallFilterInstalled() {
        return systemCallFilterInstalledMask == LOCAL_SYSTEM_CALL_FILTER_ALL;
    }

    /** Default impl that does nothing (and logs a warning message) when there is no underlying native support.*/
    static final class UnsupportedNativeOperationsImpl implements NativeOperations {

        @Override
        public boolean tryLockMemory() {
            logger.warn("cannot mlockall because JNA is not available");
            return false;
        }

        @Override
        public boolean definitelyRunningAsRoot() {
            logger.warn("cannot check if running as root because JNA is not available");
            return false;
        }

        @Override
        public String getShortPathName(String path) {
            logger.warn("cannot obtain short path for [{}] because JNA is not available", path);
            return null;
        }

        @Override
        public void addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
            logger.warn("cannot register console handler because JNA is not available");
        }

        @Override
        public OptionalLong tryRetrieveMaxNumberOfThreads() {
            logger.warn("cannot setrlimit RLIMIT_NPROC because JNA is not available");
            return OptionalLong.empty();
        }

        @Override
        public OptionalLong tryRetrieveMaxVirtualMemorySize() {
            logger.warn("cannot setrlimit RLIMIT_AS because JNA is not available");
            return OptionalLong.empty();
        }

        @Override
        public OptionalLong tryRetrieveMaxFileSize() {
            logger.warn("cannot setrlimit RLIMIT_FSIZE because JNA is not available");
            return OptionalLong.empty();
        }

        @Override
        public int tryInstallSystemCallFilter(Path tmpFile) {
            logger.warn("cannot install system call filter because JNA is not available");
            return 0;
        }
    }
}
