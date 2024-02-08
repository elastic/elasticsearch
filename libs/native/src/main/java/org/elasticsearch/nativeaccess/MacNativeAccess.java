/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.nativeaccess.lib.MacCLibrary;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary.RLimit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.OptionalLong;

class MacNativeAccess extends PosixNativeAccess {

    /** The only supported flag... */
    static final int SANDBOX_NAMED = 1;
    /** Allow everything except process fork and execution */
    static final String SANDBOX_RULES = "(version 1) (allow default) (deny process-fork) (deny process-exec)";

    // not a standard limit, means something different on linux, etc!
    static final int RLIMIT_NPROC = 7;

    private static final int F_PREALLOCATE = 42;
    private static final int F_ALLOCATECONTIG = 0x2; // allocate contiguous space
    private static final int F_ALLOCATEALL = 0x4; // allocate all the requested space or no space at all
    private static final int F_PEOFPOSMODE = 3; // allocate from the physical end of the file

    private final MacCLibrary macLibc;

    MacNativeAccess(NativeLibraryProvider libraryProvider) {
        super("MacOS", libraryProvider, 6, 9223372036854775807L, 5, 144, 96, 512);
        this.macLibc = libraryProvider.getLibrary(MacCLibrary.class);
    }

    @Override
    protected void logMemoryLimitInstructions() {
        // we don't have instructions for macos
    }

    @Override
    protected boolean nativePreallocate(int fd, long currentSize, long newSize) {
        var fst = macLibc.newFStore();
        fst.set_flags(F_ALLOCATECONTIG);
        fst.set_posmode(F_PEOFPOSMODE);
        fst.set_offset(0);
        fst.set_length(newSize);
        // first, try allocating contiguously
        if (macLibc.fcntl(fd, F_PREALLOCATE, fst) != 0) {
            // TODO: log warning?
            // that failed, so let us try allocating non-contiguously
            fst.set_flags(F_ALLOCATEALL);
            if (macLibc.fcntl(fd, F_PREALLOCATE, fst) != 0) {
                // i'm afraid captain dale had to bail
                logger.warn("Could not allocate non-contiguous size: " + libc.strerror(libc.errno()));
                return false;
            }
        }
        if (macLibc.ftruncate(fd, newSize) != 0) {
            logger.warn("Could not truncate file: " + libc.strerror(libc.errno()));
            return false;
        }
        return true;
    }

    @Override
    public void tryInitMaxNumberOfThreads() {
        // On mac the rlimit for NPROC is processes, unlike in linux where it is threads.
        // So on mac NPROC is used in conjunction with syscall filtering.
    }

    @Override
    public OptionalLong allocatedSizeInBytes(Path path) {
        // nothing like xstat for macos?
        return OptionalLong.empty();
    }

    @Override
    public int sd_notify(int unset_environment, String state) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tryInstallExecSandbox() {
        initBsdSandbox();
        initMacSandbox();
        execSandboxState = ExecSandboxState.ALL_THREADS;
    }

    @SuppressForbidden(reason = "Java tmp dir is ok")
    private static Path createTempRulesFile() throws IOException {
        return Files.createTempFile("es", "sb");
    }

    private void initMacSandbox() {
        // write rules to a temporary file, which will be passed to sandbox_init()
        Path rules;
        try {
            rules = createTempRulesFile();
            Files.write(rules, Collections.singleton(SANDBOX_RULES));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try {
            var errorRef = macLibc.newErrorReference();
            int ret = macLibc.sandbox_init(rules.toAbsolutePath().toString(), SANDBOX_NAMED, errorRef);
            // if sandbox_init() fails, add the message from the OS (e.g. syntax error) and free the buffer
            if (ret != 0) {
                RuntimeException e = new UnsupportedOperationException("sandbox_init(): " + errorRef.toString());
                macLibc.sandbox_free_error(errorRef);
                throw e;
            }
            logger.debug("OS X seatbelt initialization successful");
        } finally {
            IOUtils.deleteFilesIgnoringExceptions(rules);
        }
    }

    private void initBsdSandbox() {
        RLimit limit = libc.newRLimit();
        limit.rlim_cur(0);
        limit.rlim_max(0);
        if (libc.setrlimit(RLIMIT_NPROC, limit) != 0) {
            throw new UnsupportedOperationException("RLIMIT_NPROC unavailable: " + libc.strerror(libc.errno()));
        }

        logger.debug("BSD RLIMIT_NPROC initialization successful");
    }
}
