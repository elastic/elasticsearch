/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.collect;

import com.carrotsearch.hppc.BufferAllocationException;
import com.carrotsearch.hppc.ObjectArrayDeque;

/**
 * This is an  {@link ObjectArrayDeque} that allows indexed O(1) access to its contents.
 * Additionally, it overrides the catching on {@link OutOfMemoryError} on resize.
 *
 * @param <KType> the types contained in the deque
 */
public class IndexedArrayDeque<KType> extends ObjectArrayDeque<KType> {

    public IndexedArrayDeque(int initialSize) {
        super(initialSize);
    }

    public KType get(int i) {
        Object[] rawBuffer = buffer;
        int actualIndex = (head + i) % rawBuffer.length;
        @SuppressWarnings("unchecked")
        KType value = (KType) rawBuffer[actualIndex];
        return value;
    }

    @Override
    protected void ensureBufferSpace(int expectedAdditions) {
        try {
            super.ensureBufferSpace(expectedAdditions);
        } catch (BufferAllocationException e) {
            Throwable cause = e.getCause();
            if (cause instanceof OutOfMemoryError) {
                throw (OutOfMemoryError) cause;
            }
            throw e;
        }
    }
}
