/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.foreign.Variadic;

import java.lang.foreign.MemorySegment;

/**
 * FFM binding for the macOS sandbox API.
 *
 * <p>Both symbols are resolved from the system/default lookup (no dylib to load).
 */
@LibrarySpecification(unavailableOn = { Platform.LINUX_X64, Platform.LINUX_AARCH64, Platform.WINDOWS_X64 })
public interface MacCLibrary {

    /**
     * Checks whether the process with the given pid is subject to a sandbox policy.
     *
     * <p>Pass {@code null} for {@code operation} and {@code 0} for {@code type} to test
     * whether any sandbox is active without checking a specific operation. Returns non-zero
     * if sandboxed, 0 if not sandboxed, -1 on error.
     *
     * <p>This function is not part of the public macOS SDK but is considered stable in
     * practice: Apple has not deprecated it (unlike {@code sandbox_init}), it resolves
     * directly from the system lookup, and it is relied upon by Chromium
     * ({@code sandbox/mac/seatbelt.cc}) to detect whether the process is already
     * sandboxed before attempting further sandbox configuration.
     *
     * @see <a href="https://github.com/chromium/chromium/blob/main/sandbox/mac/seatbelt.cc">
     *      Chromium seatbelt.cc — IsSandboxed()</a>
     */
    @Variadic(firstArg = 3)
    @Function("sandbox_check")
    int sandbox_check(int pid, String operation, int type);

    /**
     * Initializes the macOS Seatbelt sandbox with the given profile file path and flags.
     * On failure, writes a pointer to an error string into {@code errorbuf}.
     *
     * <p>Deprecated by Apple in the macOS SDK, but no replacement exists. Chromium likewise
     * suppresses the deprecation warning and continues to use this call.
     *
     * @see <a href="x-man-page://3/sandbox_init">sandbox_init(3)</a>
     */
    @Function("sandbox_init")
    int sandbox_init(String profile, long flags, MemorySegment errorbuf);

    /**
     * Releases the error string buffer allocated by {@code sandbox_init} on failure.
     *
     * <p>Deprecated by Apple alongside {@code sandbox_init}; no replacement exists.
     *
     * @see <a href="x-man-page://3/sandbox_init">sandbox_init(3)</a>
     */
    @Function("sandbox_free_error")
    void sandbox_free_error(MemorySegment errstr);
}
