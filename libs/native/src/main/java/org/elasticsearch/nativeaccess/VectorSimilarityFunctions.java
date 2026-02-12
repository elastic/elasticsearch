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

    enum Function {
        /**
         * Cosine distance (byte vectors only)
         */
        COSINE,
        /**
         * Dot product distance
         */
        DOT_PRODUCT,
        /**
         * Squared Euclidean distance
         */
        SQUARE_DISTANCE
    }

    enum DataType {
        /**
         * Unsigned int7. Single vector score returns results as an int.
         */
        INT7U(Byte.BYTES),
        /**
         * 1-byte int. Single vector score returns results as an int.
         */
        INT8(Byte.BYTES),
        /**
         * 4-byte float. Single vector score returns results as a float.
         */
        FLOAT32(Float.BYTES);

        private final int bytes;

        DataType(int bytes) {
            this.bytes = bytes;
        }

        public int bytes() {
            return bytes;
        }
    }

    /**
     * The various flavors of BBQ indices. Single vector score returns results as a long.
     */
    enum BBQType {
        /**
         * 1-bit data, 4-bit queries
         */
        D1Q4((byte) 1),
        /**
         * 2-bit data, 4-bit queries
         */
        D2Q4((byte) 2),
        /**
         * 4-bit data, 4-bit queries
         */
        D4Q4((byte) 4);

        private final byte dataBits;

        BBQType(byte dataBits) {
            this.dataBits = dataBits;
        }

        public byte dataBits() {
            return dataBits;
        }

        public byte queryBits() {
            return 4;
        }
    }

    enum Operation {
        /**
         * Scores a single vector against another.
         * <p>
         * Method handle takes arguments {@code (MemorySegment, MemorySegment, int)}:
         * <ol>
         *     <li>First vector</li>
         *     <li>Second vector</li>
         *     <li>Number of dimensions, or for bbq, the number of index bytes</li>
         * </ol>
         * Return value type is determined by the {@link DataType}.
         */
        SINGLE,
        /**
         * Scores multiple vectors against a single vector.
         * <p>
         * Method handle takes arguments {@code (MemorySegment, MemorySegment, int, int, MemorySegment}:
         * <ol>
         *     <li>Multiple vectors to score {@code a}</li>
         *     <li>Single vector to score against</li>
         *     <li>Number of dimensions, or for bbq, the number of index bytes</li>
         *     <li>Number of vectors in {@code a}</li>
         *     <li>Score results, as 4-byte floats</li>
         * </ol>
         */
        BULK,
        /**
         * Scores multiple vectors against a single vector, with an offset array to determine the vectors to score.
         * <p>
         * Method handle takes arguments {@code (MemorySegment, MemorySegment, int, int, MemorySegment, int, MemorySegment}:
         * <ol>
         *     <li>Multiple vectors to score</li>
         *     <li>Single vector to score against</li>
         *     <li>Number of dimensions, or for bbq, the number of index bytes</li>
         *     <li>Number of bytes between the start of one vector and the start of the next vector in {@code a}</li>
         *     <li>Array of 4-byte ints containing indices of vectors to score in {@code a}</li>
         *     <li>Number of vectors to score</li>
         *     <li>Score results, as 4-byte floats, in order of iteration through the offset array</li>
         * </ol>
         */
        BULK_OFFSETS
    }

    MethodHandle getHandle(Function function, DataType dataType, Operation operation);

    MethodHandle getHandle(Function function, BBQType bbqType, Operation operation);

    MethodHandle applyCorrectionsEuclideanBulk();

    MethodHandle applyCorrectionsMaxInnerProductBulk();

    MethodHandle applyCorrectionsDotProductBulk();
}
