/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors.es910.internal.hppc;

import org.apache.lucene.internal.hppc.BufferAllocationException;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.lucene.util.BitUtil.nextHighestPowerOfTwo;

/**
 * Constants for primitive maps.
 *
 * @lucene.internal
 */
class HashContainers {

    static final int DEFAULT_EXPECTED_ELEMENTS = 4;

    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /** Minimal sane load factor (99 empty slots per 100). */
    static final float MIN_LOAD_FACTOR = 1 / 100.0f;

    /** Maximum sane load factor (1 empty slot per 100). */
    static final float MAX_LOAD_FACTOR = 99 / 100.0f;

    /** Minimum hash buffer size. */
    static final int MIN_HASH_ARRAY_LENGTH = 4;

    /**
     * Maximum array size for hash containers (power-of-two and still allocable in Java, not a
     * negative int).
     */
    static final int MAX_HASH_ARRAY_LENGTH = 0x80000000 >>> 1;

    static final AtomicInteger ITERATION_SEED = new AtomicInteger();

    static int iterationIncrement(int seed) {
        return 29 + ((seed & 7) << 1); // Small odd integer.
    }

    static int nextBufferSize(int arraySize, int elements, double loadFactor) {
        assert checkPowerOfTwo(arraySize);
        if (arraySize == MAX_HASH_ARRAY_LENGTH) {
            throw new BufferAllocationException(
                "Maximum array size exceeded for this load factor (elements: %d, load factor: %f)",
                elements,
                loadFactor
            );
        }

        return arraySize << 1;
    }

    static int expandAtCount(int arraySize, double loadFactor) {
        assert checkPowerOfTwo(arraySize);
        // Take care of hash container invariant (there has to be at least one empty slot to ensure
        // the lookup loop finds either the element or an empty slot).
        return Math.min(arraySize - 1, (int) Math.ceil(arraySize * loadFactor));
    }

    static boolean checkPowerOfTwo(int arraySize) {
        // These are internals, we can just assert without retrying.
        assert arraySize > 1;
        assert nextHighestPowerOfTwo(arraySize) == arraySize;
        return true;
    }

    static int minBufferSize(int elements, double loadFactor) {
        if (elements < 0) {
            throw new IllegalArgumentException("Number of elements must be >= 0: " + elements);
        }

        long length = (long) Math.ceil(elements / loadFactor);
        if (length == elements) {
            length++;
        }
        length = Math.max(MIN_HASH_ARRAY_LENGTH, nextHighestPowerOfTwo(length));

        if (length > MAX_HASH_ARRAY_LENGTH) {
            throw new BufferAllocationException(
                "Maximum array size exceeded for this load factor (elements: %d, load factor: %f)",
                elements,
                loadFactor
            );
        }

        return (int) length;
    }

    static void checkLoadFactor(double loadFactor, double minAllowedInclusive, double maxAllowedInclusive) {
        if (loadFactor < minAllowedInclusive || loadFactor > maxAllowedInclusive) {
            throw new BufferAllocationException(
                "The load factor should be in range [%.2f, %.2f]: %f",
                minAllowedInclusive,
                maxAllowedInclusive,
                loadFactor
            );
        }
    }
}
