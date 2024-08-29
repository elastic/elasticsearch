/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

class NoopNativeAccess implements NativeAccess {

    private static final Logger logger = LogManager.getLogger(NativeAccess.class);

    NoopNativeAccess() {}

    @Override
    public boolean definitelyRunningAsRoot() {
        logger.warn("Cannot check if running as root because native access is not available");
        return false;
    }

    @Override
    public ProcessLimits getProcessLimits() {
        logger.warn("Cannot get process limits because native access is not available");
        return new ProcessLimits(ProcessLimits.UNKNOWN, ProcessLimits.UNKNOWN, ProcessLimits.UNKNOWN);
    }

    @Override
    public void tryLockMemory() {
        logger.warn("Cannot lock memory because native access is not available");
    }

    @Override
    public boolean isMemoryLocked() {
        return false;
    }

    @Override
    public void tryInstallExecSandbox() {
        logger.warn("Cannot install system call filter because native access is not available");
    }

    @Override
    public ExecSandboxState getExecSandboxState() {
        return ExecSandboxState.NONE;
    }

    @Override
    public OptionalLong allocatedSizeInBytes(Path path) {
        logger.warn("Cannot get allocated size of file [" + path + "] because native access is not available");
        return OptionalLong.empty();
    }

    @Override
    public void tryPreallocate(Path file, long size) {
        logger.warn("Cannot preallocate file size because native access is not available");
    }

    @Override
    public Systemd systemd() {
        logger.warn("Cannot get systemd access because native access is not available");
        return null;
    }

    @Override
    public Zstd getZstd() {
        logger.warn("cannot compress with zstd because native access is not available");
        return null;
    }

    @Override
    public CloseableByteBuffer newBuffer(int len) {
        logger.warn("cannot allocate buffer because native access is not available");
        return null;
    }

    @Override
    public Optional<VectorSimilarityFunctions> getVectorSimilarityFunctions() {
        logger.warn("cannot get vector distance because native access is not available");
        return Optional.empty();
    }
}
