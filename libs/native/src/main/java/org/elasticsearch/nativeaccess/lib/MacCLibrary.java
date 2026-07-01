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

import java.lang.foreign.MemorySegment;

/**
 * FFM binding for the macOS sandbox API.
 *
 * <p>Both symbols are resolved from the system/default lookup (no dylib to load).
 */
@LibrarySpecification(unavailableOn = { Platform.LINUX_X64, Platform.LINUX_AARCH64, Platform.WINDOWS_X64 })
public interface MacCLibrary {

    /**
     * Initializes the macOS Seatbelt sandbox with the given profile file path and flags.
     * On failure, writes a pointer to an error string into {@code errorbuf}.
     *
     * @see <a href="x-man-page://3/sandbox_init">sandbox_init(3)</a>
     */
    @Function("sandbox_init")
    int sandbox_init(String profile, long flags, MemorySegment errorbuf);

    /**
     * Releases the error string buffer allocated by {@code sandbox_init} on failure.
     *
     * @see <a href="x-man-page://3/sandbox_init">sandbox_init(3)</a>
     */
    @Function("sandbox_free_error")
    void sandbox_free_error(MemorySegment errstr);
}
