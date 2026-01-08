/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;

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
     * Run the given callback if the current platform is POSIX.
     * @param callback A callback consuming a Posix-specific native access instance
     */
    static void onPosix(Consumer<PosixNativeAccess> callback) {
        if (NativeAccessHolder.INSTANCE instanceof PosixNativeAccess) {
            callback.accept((PosixNativeAccess) NativeAccessHolder.INSTANCE);
        }
    }

    /**
     * Run the given callback if the current platform is POSIX and return a value.
     * @param callback A callback consuming a Posix-specific native access instance and returning a value
     * @param defaultValue The value to return if the current platform is not POSIX
     * @return The result of the callback or the default value if the platform is not POSIX
     */
    static <T> T onPosix(Function<PosixNativeAccess, T> callback, T defaultValue) {
        if (NativeAccessHolder.INSTANCE instanceof PosixNativeAccess) {
            return callback.apply((PosixNativeAccess) NativeAccessHolder.INSTANCE);
        } else {
            return defaultValue;
        }
    }

    /**
     * Run the given callback if the current platform is Windows.
     * @param callback A callback consuming a Windows-specific native access instance
     */
    static void onWindows(Consumer<WindowsNativeAccess> callback) {
        if (NativeAccessHolder.INSTANCE instanceof WindowsNativeAccess) {
            callback.accept((WindowsNativeAccess) NativeAccessHolder.INSTANCE);
        }
    }

    /**
     * Run the given callback if the current platform is Windows and return a value.
     * @param callback A callback consuming a Windows-specific native access instance and returning a value
     * @param defaultValue The value to return if the current platform is not Windows
     * @return The result of the callback or the default value if the platform is not Windows
     */
    static <T> T onWindows(Function<WindowsNativeAccess, T> callback, T defaultValue) {
        if (NativeAccessHolder.INSTANCE instanceof WindowsNativeAccess) {
            return callback.apply((WindowsNativeAccess) NativeAccessHolder.INSTANCE);
        } else {
            return defaultValue;
        }
    }

    /**
     * Run the given callback if the current platform is Mac.
     * @param callback A callback consuming a Mac-specific native access instance
     */
    static void onMac(Consumer<MacNativeAccess> callback) {
        if (NativeAccessHolder.INSTANCE instanceof MacNativeAccess) {
            callback.accept((MacNativeAccess) NativeAccessHolder.INSTANCE);
        }
    }

    /**
     * Run the given callback if the current platform is Mac and return a value.
     * @param callback A callback consuming a Mac-specific native access instance and returning a value
     * @param defaultValue The value to return if the current platform is not Mac
     * @return The result of the callback or the default value if the platform is not Mac
     */
    static <T> T onMac(Function<MacNativeAccess, T> callback, T defaultValue) {
        if (NativeAccessHolder.INSTANCE instanceof MacNativeAccess) {
            return callback.apply((MacNativeAccess) NativeAccessHolder.INSTANCE);
        } else {
            return defaultValue;
        }
    }

    /**
     * Run the given callback if the current platform is Linux.
     * @param callback A callback consuming a Linux-specific native access instance
     */
    static void onLinux(Consumer<LinuxNativeAccess> callback) {
        if (NativeAccessHolder.INSTANCE instanceof LinuxNativeAccess) {
            callback.accept((LinuxNativeAccess) NativeAccessHolder.INSTANCE);
        }
    }

    /**
     * Run the given callback if the current platform is Linux and return a value.
     * @param callback A callback consuming a Linux-specific native access instance and returning a value
     * @param defaultValue The value to return if the current platform is not Linux
     * @return The result of the callback or the default value if the platform is not Linux
     */
    static <T> T onLinux(Function<LinuxNativeAccess, T> callback, T defaultValue) {
        if (NativeAccessHolder.INSTANCE instanceof LinuxNativeAccess) {
            return callback.apply((LinuxNativeAccess) NativeAccessHolder.INSTANCE);
        } else {
            return defaultValue;
        }
    }

    /**
     * Determine whether this JVM is running as the root user.
     *
     * @return true if running as root, or false if unsure
     */
    boolean definitelyRunningAsRoot();

    /**
     * Return limits for the current process.
     */
    ProcessLimits getProcessLimits();

    /**
     * Attempt to lock this process's virtual memory address space into physical RAM.
     */
    void tryLockMemory();

    /**
     * Return whether locking memory was successful, or false otherwise.
     */
    boolean isMemoryLocked();

    /**
     * Attempts to install a system call filter to block process execution.
     */
    void tryInstallExecSandbox();

    /**
     * Return whether installing the exec system call filters was successful, and to what degree.
     */
    ExecSandboxState getExecSandboxState();

    Systemd systemd();

    /**
     * Returns an accessor to zstd compression functions.
     * @return an object used to compress and decompress bytes using zstd
     */
    Zstd getZstd();

    /**
     * Retrieves the actual number of bytes of disk storage used to store a specified file.
     *
     * @param path the path to the file
     * @return an {@link OptionalLong} that contains the number of allocated bytes on disk for the file, or empty if the size is invalid
     */
    OptionalLong allocatedSizeInBytes(Path path);

    void tryPreallocate(Path file, long size);

    /*
     * Returns the vector similarity functions, or an empty optional.
     */
    Optional<VectorSimilarityFunctions> getVectorSimilarityFunctions();

    /**
     * Creates a new {@link CloseableByteBuffer} using a shared arena. The buffer can be used
     * across multiple threads.
     * @param len the number of bytes the buffer should allocate
     * @return the buffer
     */
    CloseableByteBuffer newSharedBuffer(int len);

    /**
     * Creates a new {@link CloseableByteBuffer} using a confined arena. The buffer must be
     * used within the same thread that it is created.
     * @param len the number of bytes the buffer should allocate
     * @return the buffer
     */
    CloseableByteBuffer newConfinedBuffer(int len);

    /**
     * Possible stats for execution filtering.
     */
    enum ExecSandboxState {
        /** No execution filtering */
        NONE,
        /** Exec is blocked for threads that were already created */
        EXISTING_THREADS,
        /** Exec is blocked for all current and future threads */
        ALL_THREADS
    }
}
