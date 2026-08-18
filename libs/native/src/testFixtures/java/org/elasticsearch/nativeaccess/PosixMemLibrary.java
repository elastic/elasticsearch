/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.foreign.CaptureSystemError;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.Platform;

import java.lang.foreign.MemorySegment;

/**
 * FFM binding for the POSIX virtual memory primitives used by {@link PosixGuardPageAllocator}.
 *
 * <p>Test-only by design: these are powerful primitives with no production caller, so the binding lives
 * in the {@code testFixtures} source set instead of adding surface to {@code libs/native} main. All three
 * symbols resolve from the system/default lookup, so there is no library to load.
 */
@LibrarySpecification(unavailableOn = { Platform.WINDOWS_X64 })
public interface PosixMemLibrary {

    /**
     * Creates a new mapping in the virtual address space of the calling process. Pass
     * {@link MemorySegment#NULL} as {@code addr} to let the kernel pick the address.
     *
     * @return a zero-length segment whose address is the start of the new mapping, or a segment at
     *         address {@code -1} ({@code MAP_FAILED}) on failure, with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/mmap.2.html">mmap manpage</a>
     */
    @CaptureSystemError
    @Function("mmap")
    MemorySegment mmap(MemorySegment addr, long length, int prot, int flags, int fd, long offset);

    /**
     * Changes the access protection of the pages covering {@code [addr, addr + length)}. {@code addr}
     * must be page aligned.
     *
     * @return 0 on success, -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/mprotect.2.html">mprotect manpage</a>
     */
    @CaptureSystemError
    @Function("mprotect")
    int mprotect(MemorySegment addr, long length, int prot);

    /**
     * Deletes the mappings covering {@code [addr, addr + length)}.
     *
     * @return 0 on success, -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/munmap.2.html">munmap manpage</a>
     */
    @CaptureSystemError
    @Function("munmap")
    int munmap(MemorySegment addr, long length);
}
