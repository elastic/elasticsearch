/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

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
}
