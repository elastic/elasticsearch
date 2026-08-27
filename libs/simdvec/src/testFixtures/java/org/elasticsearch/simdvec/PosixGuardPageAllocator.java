/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.foreign.LibraryProvider;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * {@link GuardPageAllocator} for POSIX platforms: reservations are anonymous {@code mmap}s, the guard page
 * is {@code mprotect}ed {@code PROT_NONE}, and each reservation is {@code munmap}ped when the arena closes.
 *
 * <p>Anonymous memory rather than a mapped file: nothing here needs to be backed by anything, and the
 * kernel already hands back zero filled pages.
 */
final class PosixGuardPageAllocator extends GuardPageAllocator {

    /**
     * Constants common on every POSIX platform we support, verified against the Linux uapi headers
     * (include/uapi/asm-generic/mman-common.h, include/uapi/linux/mman.h) and the macOS SDK's sys/mman.h.
     * MAP_ANONYMOUS is the one that differs, and it is handled by {@link PosixMemLibraryConstants}.
     */
    private static final int PROT_NONE = 0x0;
    private static final int PROT_READ = 0x1;
    private static final int PROT_WRITE = 0x2;
    private static final int MAP_PRIVATE = 0x2;
    /** {@code MAP_FAILED} is {@code (void *) -1} on both platforms. */
    private static final long MAP_FAILED = -1L;

    // Resolved once: the binding declares itself unavailable on Windows, so a null MEM_LIBRARY means
    // "this implementation cannot run here".
    private static final PosixMemLibrary MEM_LIBRARY = LibraryProvider.lookupLibrary(PosixMemLibrary.class);

    private final PosixMemLibrary memLibrary;
    private final PosixCLibrary libc;
    private final PosixMemLibraryConstants constants;
    private final int pageSize;

    static boolean supportsCurrentPlatform() {
        return MEM_LIBRARY != null;
    }

    static PosixGuardPageAllocator create(Arena delegate) {
        assert supportsCurrentPlatform();
        var constants = switch (Platform.current()) {
            case LINUX_X64, LINUX_AARCH64 -> PosixMemLibraryConstants.LINUX;
            case DARWIN_X64, DARWIN_AARCH64 -> PosixMemLibraryConstants.DARWIN;
            case WINDOWS_X64 -> throw new AssertionError("Windows is not a Posix supported platform");
        };
        return new PosixGuardPageAllocator(
            delegate,
            MEM_LIBRARY,
            NativeLibraryProvider.instance().getLibrary(PosixCLibrary.class),
            constants
        );
    }

    private PosixGuardPageAllocator(Arena delegate, PosixMemLibrary memLibrary, PosixCLibrary libc, PosixMemLibraryConstants constants) {
        super(delegate);
        this.memLibrary = memLibrary;
        this.libc = libc;
        this.constants = constants;
        this.pageSize = libc.getPageSize();
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    @SuppressWarnings("restricted") // MemorySegment.reinterpret is a restricted method; used to tie munmap to the arena scope.
    protected MemorySegment reserve(long byteSize) {
        MemorySegment base = memLibrary.mmap(
            MemorySegment.NULL, // let the kernel choose the address
            byteSize,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | constants.MAP_ANONYMOUS(),
            -1, // no file descriptor, the mapping is anonymous
            0L
        );
        if (base.address() == MAP_FAILED) {
            throw new IllegalStateException("mmap of " + byteSize + " bytes failed: " + lastError());
        }
        // reinterpret ties the munmap to the arena's scope, so closing the arena releases the reservation
        return base.reinterpret(byteSize, delegate(), this::unmap);
    }

    @Override
    protected void denyAccess(MemorySegment page) {
        if (memLibrary.mprotect(page, page.byteSize(), PROT_NONE) != 0) {
            throw new IllegalStateException("mprotect PROT_NONE of " + page.byteSize() + " bytes failed: " + lastError());
        }
    }

    private void unmap(MemorySegment region) {
        if (memLibrary.munmap(region, region.byteSize()) != 0) {
            throw new IllegalStateException("munmap of " + region.byteSize() + " bytes failed: " + lastError());
        }
    }

    private String lastError() {
        int errno = libc.errno();
        return "error=" + errno + ", reason=" + libc.strerror(errno);
    }
}
