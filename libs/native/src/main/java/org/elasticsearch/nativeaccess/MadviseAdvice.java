/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

/**
 * Constants for use with {@link MappedSegment#madvise}.
 * Values correspond to the POSIX {@code posix_madvise} advice constants.
 */
public final class MadviseAdvice {

    /** Default access pattern; the OS applies its normal read-ahead and caching policy. */
    public static final int NORMAL = PosixCLibrary.POSIX_MADV_NORMAL;

    /** Random access pattern; disables read-ahead. */
    public static final int RANDOM = PosixCLibrary.POSIX_MADV_RANDOM;

    /** Sequential access pattern; aggressive read-ahead. */
    public static final int SEQUENTIAL = PosixCLibrary.POSIX_MADV_SEQUENTIAL;

    /** Data will be needed soon; bring pages into memory. */
    public static final int WILLNEED = PosixCLibrary.POSIX_MADV_WILLNEED;

    /** Data will not be needed soon; pages may be freed. */
    public static final int DONTNEED = PosixCLibrary.POSIX_MADV_DONTNEED;

    private MadviseAdvice() {}
}
