/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.foreign.CloseableByteBuffer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
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
        if (NativeAccessHolder.INSTANCE instanceof PosixNativeAccess posixNativeAccess) {
            callback.accept(posixNativeAccess);
        }
    }

    /**
     * Run the given callback if the current platform is POSIX and return a value.
     * @param callback A callback consuming a Posix-specific native access instance and returning a value
     * @return An optional containing the result of the callback if the platform is POSIX, or empty otherwise
     */
    static <T> Optional<T> onPosixReturn(Function<PosixNativeAccess, T> callback) {
        if (NativeAccessHolder.INSTANCE instanceof PosixNativeAccess posixNativeAccess) {
            return Optional.of(callback.apply(posixNativeAccess));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Run the given callback if the current platform is Windows.
     * @param callback A callback consuming a Windows-specific native access instance
     */
    static void onWindows(Consumer<WindowsNativeAccess> callback) {
        if (NativeAccessHolder.INSTANCE instanceof WindowsNativeAccess windowsNativeAccess) {
            callback.accept(windowsNativeAccess);
        }
    }

    /**
     * Run the given callback if the current platform is Windows and return a value.
     * @param callback A callback consuming a Windows-specific native access instance and returning a value
     * @return An optional containing the result of the callback if the platform is Windows, or empty otherwise
     */
    static <T> Optional<T> onWindowsReturn(Function<WindowsNativeAccess, T> callback) {
        if (NativeAccessHolder.INSTANCE instanceof WindowsNativeAccess windowsNativeAccess) {
            return Optional.of(callback.apply(windowsNativeAccess));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Run the given callback if the current platform is Mac.
     * @param callback A callback consuming a Mac-specific native access instance
     */
    static void onMac(Consumer<MacNativeAccess> callback) {
        if (NativeAccessHolder.INSTANCE instanceof MacNativeAccess macNativeAccess) {
            callback.accept(macNativeAccess);
        }
    }

    /**
     * Run the given callback if the current platform is Mac and return a value.
     * @param callback A callback consuming a Mac-specific native access instance and returning a value
     * @return An optional containing the result of the callback if the platform is Mac, or empty otherwise
     */
    static <T> Optional<T> onMacReturn(Function<MacNativeAccess, T> callback) {
        if (NativeAccessHolder.INSTANCE instanceof MacNativeAccess macNativeAccess) {
            return Optional.of(callback.apply(macNativeAccess));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Run the given callback if the current platform is Linux.
     * @param callback A callback consuming a Linux-specific native access instance
     */
    static void onLinux(Consumer<LinuxNativeAccess> callback) {
        if (NativeAccessHolder.INSTANCE instanceof LinuxNativeAccess linuxNativeAccess) {
            callback.accept(linuxNativeAccess);
        }
    }

    /**
     * Run the given callback if the current platform is Linux and return a value.
     * @param callback A callback consuming a Linux-specific native access instance and returning a value
     * @return An optional containing the result of the callback if the platform is Linux, or empty otherwise
     */
    static <T> Optional<T> onLinuxReturn(Function<LinuxNativeAccess, T> callback) {
        if (NativeAccessHolder.INSTANCE instanceof LinuxNativeAccess linuxNativeAccess) {
            return Optional.of(callback.apply(linuxNativeAccess));
        } else {
            return Optional.empty();
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

    /**
     * Flag for sync_file_range(2): initiate writeback for dirty pages in the given range (non-blocking).
     */
    int SYNC_FILE_RANGE_WRITE = 2;

    /**
     * Flag for sync_file_range(2): wait for writeback of pages in the given range to complete.
     */
    int SYNC_FILE_RANGE_WAIT_AFTER = 4;

    /**
     * Flushes dirty page-cache pages for the given byte range in the file at {@code path} to disk.
     * The default implementation calls {@code FileChannel.force(false)} on the whole file when
     * {@code SYNC_FILE_RANGE_WAIT_AFTER} is set, providing a correct (if range-unaware) durability
     * guarantee on non-Linux platforms.
     * Linux overrides this with a real {@code sync_file_range(2)} call.
     *
     * @param path   path to the file to flush
     * @param offset byte offset of the range start
     * @param nbytes length of the range in bytes
     * @param flags  combination of SYNC_FILE_RANGE_* flags
     * @throws IOException if the flush fails
     */
    default void syncFileRange(Path path, long offset, long nbytes, int flags) throws IOException {
        if ((flags & SYNC_FILE_RANGE_WAIT_AFTER) != 0) {
            try (FileChannel fc = FileChannel.open(path, java.nio.file.StandardOpenOption.READ, java.nio.file.StandardOpenOption.WRITE)) {
                fc.force(false);
            }
        }
    }

    void tryPreallocate(Path file, long size);

    /*
     * Returns the vector similarity functions, or an empty optional.
     */
    Optional<VectorSimilarityFunctions> getVectorSimilarityFunctions();

    /**
     * Returns Parquet-rs native functions, or an empty optional if unavailable on this platform.
     */
    Optional<ParquetRsFunctions> getParquetRsFunctions();

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
     * Creates a new {@link CloseableMappedByteBuffer} using a shared arena. The buffer can be used
     * across multiple threads, and should be closed.
     * @return the buffer
     */
    CloseableMappedByteBuffer map(FileChannel fileChannel, MapMode mode, long position, long size) throws IOException;

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
