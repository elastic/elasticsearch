/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import java.nio.file.Path;
import java.util.OptionalLong;

/**
 * Provides access to native functionality needed by Elastisearch.
 */
public interface NativeAccess {

    /**
     * Get the one and only instance of {@link NativeAccess} which is specific to the running platform and JVM.
     */
    static NativeAccess instance() {
        return NativeAccessHolder.INSTANCE;
    }

    /**
     * Determine whether this JVM is running as the root user.
     *
     * @return true if running as root, or false if unsure
     */
    boolean definitelyRunningAsRoot();

    void tryLockMemory();

    boolean isMemoryLocked();

    void tryInitMaxNumberOfThreads();

    long getMaxNumberOfThreads();

    void tryInitMaxVirtualMemorySize();

    long getMaxVirtualMemorySize();

    void tryInitMaxFileSize();

    long getMaxFileSize();

    void tryInstallExecSandbox();

    ExecSandboxState getExecSandboxState();

    /**
     * Retrieves the actual number of bytes of disk storage used to store a specified file.
     *
     * @param path the path to the file
     * @return an {@link OptionalLong} that contains the number of allocated bytes on disk for the file, or empty if the size is invalid
     */
    OptionalLong allocatedSizeInBytes(Path path);

    /**
     * Retrieves the short path form of the specified path.
     *
     * @param path the path
     * @return the short path name, or the original path name if unsupported or unavailable
     */
    String getShortPathName(String path);

    /**
     * Adds a Console Ctrl Handler for Windows. On non-windows this is a noop.
     *
     * @return true if the handler is correctly set
     */
    boolean addConsoleCtrlHandler(ConsoleCtrlHandler handler);

    void tryPreallocate(Path file, long size);

    long getRlimitInfinity();

    /**
     * Notify systemd of state changes.
     *
     * @param unset_environment if non-zero, the NOTIFY_SOCKET environment variable will be unset before returning and further calls to
     *                          sd_notify will fail
     * @param state             a new-line separated list of variable assignments; some assignments are understood directly by systemd
     * @return a negative error code on failure, and positive if status was successfully sent
     */
    int sd_notify(int unset_environment, String state);

    /*
    int preallocate(int fd, long offset, long length);
    */
    /**
     * Windows callback for console events
     */
    interface ConsoleCtrlHandler {

        int CTRL_CLOSE_EVENT = 2;

        /**
         * Handles the Ctrl event.
         *
         * @param code the code corresponding to the Ctrl sent.
         * @return true if the handler processed the event, false otherwise. If false, the next handler will be called.
         */
        boolean handle(int code);
    }

    enum ExecSandboxState {
        NONE,
        EXISTING_THREADS,
        ALL_THREADS
    }
}
