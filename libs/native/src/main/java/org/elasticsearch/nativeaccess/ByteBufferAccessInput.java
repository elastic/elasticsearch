/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import java.nio.ByteBuffer;

/**
 * An optional interface that an IndexInput can implement to provide direct
 * access to the underlying data as a {@link ByteBuffer}. This enables
 * zero-copy access to memory-mapped data for SIMD-accelerated vector scoring.
 *
 * <p>Implementations should return a read-only ByteBuffer slice of the
 * underlying mapped memory when available, or {@code null} when direct
 * access is not possible (e.g., data not cached, spans multiple regions,
 * or not memory-mapped).
 */
public interface ByteBufferAccessInput {

    /**
     * Returns a read-only ByteBuffer slice for the given range, or {@code null}
     * if direct access is not available.
     *
     * @param offset the byte offset within the input
     * @param length the number of bytes requested
     * @return a read-only ByteBuffer positioned at the start of the data, or null
     */
    ByteBuffer byteBufferSliceOrNull(long offset, long length);
}
