/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.core.IOUtils;
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

    private final MacCLibrary macLibc;

    MacNativeAccess(NativeLibraryProvider libraryProvider) {
        super(libraryProvider, 6, 9223372036854775807L, 5);
        this.macLibc = libraryProvider.getLibrary(MacCLibrary.class);
    }

    @Override
    protected void logMemoryLimitInstructions() {
        // we don't have instructions for macos
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
    public void tryInstallExecSandbox() {
        initBsdSandbox();
        initMacSandbox();
        execSandboxState = ExecSandboxState.ALL_THREADS;
    }

    private void initMacSandbox() {
        // write rules to a temporary file, which will be passed to sandbox_init()
        Path rules;
        try {
            rules = Files.createTempFile("es", "sb");
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
