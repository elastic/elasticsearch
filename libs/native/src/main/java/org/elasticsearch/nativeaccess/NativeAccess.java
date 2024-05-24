/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import java.util.Optional;

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

    Systemd systemd();

    /**
     * Returns an accessor to zstd compression functions.
     * @return an object used to compress and decompress bytes using zstd
     */
    Zstd getZstd();

    /**
     * Returns an accessor for native functions only available on Windows, or {@code null} if not on Windows.
     */
    default WindowsFunctions getWindowsFunctions() {
        return null;
    }

    /*
     * Returns the vector similarity functions, or an empty optional.
     */
    Optional<VectorSimilarityFunctions> getVectorSimilarityFunctions();

    /**
     * Creates a new {@link CloseableByteBuffer}. The buffer must be used within the same thread
     * that it is created.
     * @param len the number of bytes the buffer should allocate
     * @return the buffer
     */
    CloseableByteBuffer newBuffer(int len);
}
