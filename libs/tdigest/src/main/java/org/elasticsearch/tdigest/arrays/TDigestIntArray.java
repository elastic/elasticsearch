/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest.arrays;

import org.elasticsearch.core.Releasable;

/**
 * Minimal interface for IntArray-like classes used within TDigest.
 */
public interface TDigestIntArray extends Releasable {
    int size();

    int get(int index);

    void set(int index, int value);

    /**
     * Resizes the array. If the new size is bigger than the current size, the new elements are set to 0.
     */
    void resize(int newSize);

    /**
     * Copies {@code len} elements from {@code buf} to this array.
     * <p>
     *     As this method will be used to insert elements from itself in an insertion sort,
     *     the copy must be made in reverse order, from offset+len-1 to offset.
     * </p>
     */
    default void set(int index, TDigestIntArray buf, int offset, int len) {
        assert index >= 0 && index + len <= this.size();
        assert buf != this || index >= offset : "To set to itself, the destination index must be greater than the source offset";
        for (int i = len - 1; i >= 0; i--) {
            this.set(index + i, buf.get(offset + i));
        }
    }
}
