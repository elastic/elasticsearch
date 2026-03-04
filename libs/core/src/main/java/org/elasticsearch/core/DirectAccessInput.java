/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An optional interface that an IndexInput can implement to provide direct
 * access to the underlying data as a {@link ByteBuffer}. This enables
 * zero-copy access to memory-mapped data for SIMD-accelerated vector scoring.
 *
 * <p> The byte buffer is passed to the caller's action and is only valid for
 * the duration of that call. All ref-counting and resource releases, if any,
 * is handled internally.
 */
public interface DirectAccessInput {

    /**
     * If a direct byte buffer view is available for the given range, passes it
     * to {@code action} and returns {@code true}. Otherwise returns
     * {@code false} without invoking the action.
     *
     * <p>The byte buffer is read-only and valid only for the duration of the
     * action. Callers must not retain references to it after the action returns.
     *
     * @param offset the byte offset within the input
     * @param length the number of bytes requested
     * @param action the action to perform with the byte buffer
     * @return {@code true} if a buffer was available and the action was invoked
     */
    boolean withByteBufferSlice(long offset, long length, CheckedConsumer<ByteBuffer, IOException> action) throws IOException;
}
