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

import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Arrays;

public class BigArraysTests extends ElasticsearchTestCase {

    public void testIntArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        IntArray array = BigArrays.newIntArray(startLen);
        int[] ref = new int[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomInt();
            array = BigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(ref[i], array.get(i));
        }
    }

    public void testLongArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        LongArray array = BigArrays.newLongArray(startLen);
        long[] ref = new long[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomLong();
            array = BigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(ref[i], array.get(i));
        }
    }

    public void testDoubleArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        DoubleArray array = BigArrays.newDoubleArray(startLen);
        double[] ref = new double[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomDouble();
            array = BigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(ref[i], array.get(i), 0.001d);
        }
    }

    public void testObjectArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        ObjectArray<Object> array = BigArrays.newObjectArray(startLen);
        final Object[] pool = new Object[100];
        for (int i = 0; i < pool.length; ++i) {
            pool[i] = new Object();
        }
        Object[] ref = new Object[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomFrom(pool);
            array = BigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertSame(ref[i], array.get(i));
        }
    }

    public void testDoubleArrayFill() {
        final int len = randomIntBetween(1, 100000);
        final int fromIndex = randomIntBetween(0, len - 1);
        final int toIndex = randomBoolean()
            ? Math.min(fromIndex + randomInt(100), len) // single page
            : randomIntBetween(fromIndex, len); // likely multiple pages
        final DoubleArray array2 = BigArrays.newDoubleArray(len);
        final double[] array1 = new double[len];
        for (int i = 0; i < len; ++i) {
            array1[i] = randomDouble();
            array2.set(i, array1[i]);
        }
        final double rand = randomDouble();
        Arrays.fill(array1, fromIndex, toIndex, rand);
        array2.fill(fromIndex, toIndex, rand);
        for (int i = 0; i < len; ++i) {
            assertEquals(array1[i], array2.get(i), 0.001d);
        }
    }

}
