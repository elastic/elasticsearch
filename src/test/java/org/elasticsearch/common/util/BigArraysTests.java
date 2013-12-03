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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cache.recycler.MockPageCacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;

public class BigArraysTests extends ElasticsearchTestCase {

    public static PageCacheRecycler randomCacheRecycler() {
        return randomBoolean() ? null : new MockPageCacheRecycler(ImmutableSettings.EMPTY, new ThreadPool());
    }

    public void testByteArrayGrowth() {
        final int totalLen = randomIntBetween(1, 4000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        ByteArray array = BigArrays.newByteArray(startLen, randomCacheRecycler(), randomBoolean());
        byte[] ref = new byte[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomByte();
            array = BigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(ref[i], array.get(i));
        }
        array.release();
    }

    public void testIntArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        IntArray array = BigArrays.newIntArray(startLen, randomCacheRecycler(), randomBoolean());
        int[] ref = new int[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomInt();
            array = BigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(ref[i], array.get(i));
        }
        array.release();
    }

    public void testLongArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        LongArray array = BigArrays.newLongArray(startLen, randomCacheRecycler(), randomBoolean());
        long[] ref = new long[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomLong();
            array = BigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(ref[i], array.get(i));
        }
        array.release();
    }

    public void testDoubleArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        DoubleArray array = BigArrays.newDoubleArray(startLen, randomCacheRecycler(), randomBoolean());
        double[] ref = new double[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomDouble();
            array = BigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(ref[i], array.get(i), 0.001d);
        }
        array.release();
    }

    public void testObjectArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        ObjectArray<Object> array = BigArrays.newObjectArray(startLen, randomCacheRecycler());
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
        array.release();
    }

    public void testDoubleArrayFill() {
        final int len = randomIntBetween(1, 100000);
        final int fromIndex = randomIntBetween(0, len - 1);
        final int toIndex = randomBoolean()
            ? Math.min(fromIndex + randomInt(100), len) // single page
            : randomIntBetween(fromIndex, len); // likely multiple pages
        final DoubleArray array2 = BigArrays.newDoubleArray(len, randomCacheRecycler(), randomBoolean());
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
        array2.release();
    }

    public void testLongArrayFill() {
        final int len = randomIntBetween(1, 100000);
        final int fromIndex = randomIntBetween(0, len - 1);
        final int toIndex = randomBoolean()
            ? Math.min(fromIndex + randomInt(100), len) // single page
            : randomIntBetween(fromIndex, len); // likely multiple pages
        final LongArray array2 = BigArrays.newLongArray(len, randomCacheRecycler(), randomBoolean());
        final long[] array1 = new long[len];
        for (int i = 0; i < len; ++i) {
            array1[i] = randomLong();
            array2.set(i, array1[i]);
        }
        final long rand = randomLong();
        Arrays.fill(array1, fromIndex, toIndex, rand);
        array2.fill(fromIndex, toIndex, rand);
        for (int i = 0; i < len; ++i) {
            assertEquals(array1[i], array2.get(i));
        }
        array2.release();
    }

    public void testByteArrayBulkGet() {
        final byte[] array1 = new byte[randomIntBetween(1, 4000000)];
        getRandom().nextBytes(array1);
        final ByteArray array2 = BigArrays.newByteArray(array1.length, randomCacheRecycler(), randomBoolean());
        for (int i = 0; i < array1.length; ++i) {
            array2.set(i, array1[i]);
        }
        final BytesRef ref = new BytesRef();
        for (int i = 0; i < 1000; ++i) {
            final int offset = randomInt(array1.length - 1);
            final int len = randomInt(Math.min(randomBoolean() ? 10 : Integer.MAX_VALUE, array1.length - offset));
            array2.get(offset, len, ref);
            assertEquals(new BytesRef(array1, offset, len), ref);
        }
        array2.release();
    }

    public void testByteArrayBulkSet() {
        final byte[] array1 = new byte[randomIntBetween(1, 4000000)];
        getRandom().nextBytes(array1);
        final ByteArray array2 = BigArrays.newByteArray(array1.length, randomCacheRecycler(), randomBoolean());
        for (int i = 0; i < array1.length; ) {
            final int len = Math.min(array1.length - i, randomBoolean() ? randomInt(10) : randomInt(3 * BigArrays.BYTE_PAGE_SIZE));
            array2.set(i, array1, i, len);
            i += len;
        }
        for (int i = 0; i < array1.length; ++i) {
            assertEquals(array1[i], array2.get(i));
        }
        array2.release();
    }

}
