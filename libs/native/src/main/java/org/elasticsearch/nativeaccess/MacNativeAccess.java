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

class MacNativeAccess extends PosixNativeAccess {

    /** The only supported flag... */
    static final int SANDBOX_NAMED = 1;
    /** Allow everything except process fork and execution */
    static final String SANDBOX_RULES = "(version 1) (allow default) (deny process-fork) (deny process-exec)";

    private final MacCLibrary macLibc;

    MacNativeAccess(NativeLibraryProvider libraryProvider) {
        super("MacOS", libraryProvider, new PosixConstants(9223372036854775807L, 5, 1, 6));
        this.macLibc = libraryProvider.getLibrary(MacCLibrary.class);
    }

    @Override
    protected long getMaxThreads() {
        return ProcessLimits.UNKNOWN;
    }

    @Override
    protected void logMemoryLimitInstructions() {
        // we don't have instructions for macos
    }

    /**
     * Installs exec system call filtering on MacOS.
     * <p>
     * Two different methods of filtering are used. Since MacOS is BSD based, process creation
     * is first restricted with {@code setrlimit(RLIMIT_NPROC)}.
     * <p>
     * Additionally, on Mac OS X Leopard or above, a custom {@code sandbox(7)} ("Seatbelt") profile is installed that
     * denies the following rules:
     * <ul>
     *   <li>{@code process-fork}</li>
     *   <li>{@code process-exec}</li>
     * </ul>
     * @see <a href="https://reverse.put.as/wp-content/uploads/2011/06/The-Apple-Sandbox-BHDC2011-Paper.pdf">
     *  *      https://reverse.put.as/wp-content/uploads/2011/06/The-Apple-Sandbox-BHDC2011-Paper.pdf</a>
     */
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
        // not a standard limit, means something different on linux, etc!
        final int RLIMIT_NPROC = 7;
        if (libc.setrlimit(RLIMIT_NPROC, limit) != 0) {
            throw new UnsupportedOperationException("RLIMIT_NPROC unavailable: " + libc.strerror(libc.errno()));
        }

        logger.debug("BSD RLIMIT_NPROC initialization successful");
    }
}
