/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
     * Produces a method handle which computes the dot product of several byte (unsigned
     * int7) vectors. This bulk operation can be used to compute the dot product between a
     * single query vector and a number of other vectors.
     *
     * <p> Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
     *
     * <p> The type of the method handle will have {@code void} as return type. The type of
     * its first and second arguments will be {@code MemorySegment}, the former contains the
     * vector data bytes for several vectors, while the latter just a single vector. The
     * type of the third argument is an int, representing the dimensions of each vector. The
     * type of the fourth argument is an int, representing the number of vectors in the
     * first argument. The type of the final argument is a MemorySegment, into which the
     * computed dot product float values will be stored.
     */
    MethodHandle dotProductHandle7uBulk();

    /**
     * Produces a method handle which computes the dot product of several byte (unsigned
     * int7) vectors. This bulk operation can be used to compute the dot product between a
     * single query vector and a subset of vectors from a dataset (array of vectors). Each
     * vector to include in the operation is identified by an offset inside the dataset.
     *
     * <p> Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
     *
     * <p> The type of the method handle will have {@code void} as return type. The type of
     * its arguments will be:
     * <ol>
     *     <li>a {@code MemorySegment} containing the vector data bytes for several vectors;
     *     in other words, a contiguous array of vectors</li>
     *     <li>a {@code MemorySegment} containing the vector data bytes for a single ("query") vector</li>
     *     <li>an {@code int}, representing the dimensions of each vector</li>
     *     <li>an {@code int}, representing the width (in bytes) of each vector. Or, in other words,
     *     the distance in bytes between two vectors inside the first param's {@code MemorySegment}</li>
     *     <li>a {@code MemorySegment} containing the indices of the vectors inside the first param's array
     *     on which we'll compute the dot product</li>
     *     <li>an {@code int}, representing the number of vectors for which we'll compute the dot product
     *     (which is equal to the size - in number of elements - of the 5th and 7th {@code MemorySegment}s)</li>
     *     <li>a {@code MemorySegment}, into which the computed dot product float values will be stored</li>
     * </ol>
     */
    MethodHandle dotProductHandle7uBulkWithOffsets();

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

    /**
     * Produces a method handle returning the cosine of float32 vectors.
     *
     * <p> The type of the method handle will have {@code float} as return type, The type of
     * its first and second arguments will be {@code MemorySegment}, whose contents is the
     * vector data floats. The third argument is the length of the vector data - number of
     * 4-byte float32 elements.
     */
    MethodHandle cosineHandleFloat32();

    /**
     * Produces a method handle returning the dot product of float32 vectors.
     *
     * <p> The type of the method handle will have {@code float} as return type, The type of
     * its first and second arguments will be {@code MemorySegment}, whose contents is the
     * vector data floats. The third argument is the length of the vector data - number of
     * 4-byte float32 elements.
     */
    MethodHandle dotProductHandleFloat32();

    /**
     * Produces a method handle returning the square distance of float32 vectors.
     *
     * <p> The type of the method handle will have {@code float} as return type, The type of
     * its first and second arguments will be {@code MemorySegment}, whose contents is the
     * vector data floats. The third argument is the length of the vector data - number of
     * 4-byte float32 elements.
     */
    MethodHandle squareDistanceHandleFloat32();
}
