/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.util;

import java.util.Arrays;

/**
 * Utility methods to work with {@link IntArray}s.
 */
public class IntArrays {

    private IntArrays() {
    }

    /**
     * Return a {@link IntArray} view over the provided array.
     */
    public static IntArray wrap(final int[] array) {
        return new IntArray() {

            private void checkIndex(long index) {
                if (index > Integer.MAX_VALUE) {
                    throw new IndexOutOfBoundsException(Long.toString(index));
                }
            }

            @Override
            public void set(long index, int value) {
                checkIndex(index);
                array[(int) index] = value;
            }

            @Override
            public int increment(long index, int inc) {
                checkIndex(index);
                return array[(int) index] += inc;
            }

            @Override
            public int get(long index) {
                checkIndex(index);
                return array[(int) index];
            }

            @Override
            public void clear(int sentinal) {
                Arrays.fill(array, sentinal);
            }
        };
    }

    /**
     * Return a newly allocated {@link IntArray} of the given length or more.
     */
    public static IntArray allocate(long length) {
        if (length <= BigIntArray.DEFAULT_PAGE_SIZE) {
            return wrap(new int[(int) length]);
        } else {
            return new BigIntArray(length);
        }
    }

}
