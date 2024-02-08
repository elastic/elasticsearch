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

class NoopNativeAccess extends AbstractNativeAccess {

    NoopNativeAccess() {
        super("noop");
    }

    @Override
    public boolean definitelyRunningAsRoot() {
        logger.warn("Cannot check if running as root because native access is not available");
        return false;
    }

    @Override
    public void tryLockMemory() {
        logger.warn("Cannot lock memory because native access is not available");
    }

    public void tryInitMaxNumberOfThreads() {
        logger.warn("Cannot init max number of threads because native access is not available");
    }

    public void tryInitMaxVirtualMemorySize() {
        logger.warn("Cannot init max size of virtual memory because native access is not available");
    }

    public void tryInitMaxFileSize() {
        logger.warn("Cannot init max file size because native access is not available");
    }

    @Override
    public void tryInstallExecSandbox() {
        logger.warn("Cannot install system call filter because native access is not available");
    }

    @Override
    public OptionalLong allocatedSizeInBytes(Path path) {
        logger.warn("Cannot get allocated size of file [" + path + "] because native access is not available");
        return OptionalLong.empty();
    }

    @Override
    public String getShortPathName(String path) {
        logger.warn("Cannot get short path name for file [" + path + "] because native access is not available");
        return path;
    }

    @Override
    public boolean addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        logger.warn("Cannot register console control handler because native access is not available");
        return false;
    }

    @Override
    public void tryPreallocate(Path file, long size) {
        logger.warn("Cannot preallocate file size because native access is not available");
    }

    @Override
    public long getRlimitInfinity() {
        return -1L;
    }

    @Override
    public int sd_notify(int unset_environment, String state) {
        throw new UnsupportedOperationException();
    }
}
