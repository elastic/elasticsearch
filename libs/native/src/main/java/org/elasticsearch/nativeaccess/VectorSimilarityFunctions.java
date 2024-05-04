/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import java.lang.invoke.MethodHandle;

/**
 * Utility interface providing vector similarity functions.
 *
 * <p> MethodHandles are returned to avoid a static reference to MemorySegment,
 * which is not in the currently lowest compile version, JDK 17. Code consuming
 * the method handles will, by definition, require access to MemorySegment.
 */
public interface VectorSimilarityFunctions {
    /**
     * Produces a method handle returning the dot product of byte (unsigned int7) vectors.
     *
     * <p> Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
     *
     * <p> The type of the method handle will have {@code int} as return type, The type of
     * its first and second arguments will be {@code MemorySegment}, whose contents is the
     * vector data bytes. The third argument is the length of the vector data.
     */
    MethodHandle dotProductHandle7u();

    /**
     * Produces a method handle returning the square distance of byte (unsigned int7) vectors.
     *
     * <p> Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
     *
     * <p> The type of the method handle will have {@code int} as return type, The type of
     * its first and second arguments will be {@code MemorySegment}, whose contents is the
     * vector data bytes. The third argument is the length of the vector data.
     */
    MethodHandle squareDistanceHandle7u();
}
