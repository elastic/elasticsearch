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

import org.apache.lucene.internal.hppc.BitMixer;
import org.apache.lucene.internal.hppc.FloatArrayList;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.index.codec.vectors.es910.util.ArrayUtil;

import static org.elasticsearch.index.codec.vectors.es910.internal.hppc.HashContainers.DEFAULT_EXPECTED_ELEMENTS;

/**
 * An array-backed list of {@code float} with a maximum size limit.
 *
 * @lucene.internal
 */
public class MaxSizedFloatArrayList extends FloatArrayList {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MaxSizedFloatArrayList.class);

    final int maxSize;

    /** New instance with sane defaults. */
    public MaxSizedFloatArrayList(int maxSize) {
        this(maxSize, DEFAULT_EXPECTED_ELEMENTS);
    }

    /**
     * New instance with sane defaults.
     *
     * @param maxSize The maximum size this list can grow to
     * @param expectedElements The expected number of elements guaranteed not to cause buffer
     *     expansion (inclusive).
     */
    public MaxSizedFloatArrayList(int maxSize, int expectedElements) {
        super(expectedElements);
        assert expectedElements <= maxSize : "expectedElements (" + expectedElements + ") must be <= maxSize (" + maxSize + ")";
        this.maxSize = maxSize;
    }

    /** Creates a new list from the elements of another list in its iteration order. */
    public MaxSizedFloatArrayList(MaxSizedFloatArrayList list) {
        super(list.size());
        this.maxSize = list.maxSize;
        addAll(list);
    }

    @Override
    protected void ensureBufferSpace(int expectedAdditions) {
        if (elementsCount + expectedAdditions > maxSize) {
            throw new IllegalStateException("Cannot grow beyond maxSize: " + maxSize);
        }
        if (elementsCount + expectedAdditions > buffer.length) {
            this.buffer = ArrayUtil.growInRange(buffer, elementsCount + expectedAdditions, maxSize);
        }
    }

    @Override
    public int hashCode() {
        int h = 1, max = elementsCount;
        h = 31 * h + maxSize;
        for (int i = 0; i < max; i++) {
            h = 31 * h + BitMixer.mix(this.buffer[i]);
        }
        return h;
    }

    /**
     * Returns <code>true</code> only if the other object is an instance of the same class and with
     * the same elements and maxSize.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MaxSizedFloatArrayList other = (MaxSizedFloatArrayList) obj;
        return maxSize == other.maxSize && super.equals(obj);
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(buffer);
    }
}
