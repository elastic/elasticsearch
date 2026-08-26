/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

/**
 * The mem function constants that differ between POSIX platforms. Constants that are the same on
 * every POSIX platform we support ({@code PROT_*}, {@code MAP_PRIVATE}, {@code MAP_FAILED}) live in
 * {@link PosixGuardPageAllocator} instead.
 */
record PosixMemLibraryConstants(int MAP_ANONYMOUS) {

    /** {@code MAP_ANONYMOUS} from {@code include/uapi/asm-generic/mman-common.h} (x64 and aarch64 both use the generic value). */
    static final PosixMemLibraryConstants LINUX = new PosixMemLibraryConstants(0x20);

    /** {@code MAP_ANON} from the macOS SDK's {@code sys/mman.h}; {@code MAP_ANONYMOUS} is an alias of it. */
    static final PosixMemLibraryConstants DARWIN = new PosixMemLibraryConstants(0x1000);
}
