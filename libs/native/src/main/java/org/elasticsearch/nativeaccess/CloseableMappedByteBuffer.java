/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.foreign.CloseableByteBuffer;

/** A closeable buffer backed by a mapped file. */
public interface CloseableMappedByteBuffer extends CloseableByteBuffer {

    /**
     * Returns a slice of this buffer. Closing a slice does not close it's parent.
     */
    CloseableMappedByteBuffer slice(long index, long length);

    /**
     * Prefetches the given offset and length.
     */
    void prefetch(long offset, long length);

    /**
     * Advises the operating system about the expected access pattern for the
     * specified memory region, allowing the kernel to optimize paging behavior.
     *
     * @param offset the starting offset within the buffer
     * @param length the length of the region in bytes
     * @param advice the access pattern advice; see {@link MadviseAdvice} constants
     */
    void madvise(long offset, long length, int advice);
}
