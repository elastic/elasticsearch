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
 * A wrapper around a native {@link ByteBuffer} which allows that buffer to be
 * closed synchronously. This is in contrast to JDK created native buffers
 * which are deallocated only after GC has cleaned up references to
 * the buffer.
 */
public interface CloseableByteBuffer extends AutoCloseable {
    /**
     * Returns the wrapped {@link ByteBuffer}.
     */
    ByteBuffer buffer();

    @Override
    void close();
}
