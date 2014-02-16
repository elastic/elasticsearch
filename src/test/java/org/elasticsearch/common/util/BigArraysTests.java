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

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cache.recycler.MockPageCacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.cache.recycler.MockBigArrays;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.Arrays;

public class BigArraysTests extends ElasticsearchTestCase {

    public static BigArrays randombigArrays() {
        final PageCacheRecycler recycler = randomBoolean() ? null : new MockPageCacheRecycler(ImmutableSettings.EMPTY, new ThreadPool());
        return new MockBigArrays(ImmutableSettings.EMPTY, recycler);
    }

    private BigArrays bigArrays;

    @Before
    public void init() {
        bigArrays = randombigArrays();
    }

    public void testByteArrayGrowth() {
        final int totalLen = randomIntBetween(1, 4000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        ByteArray array = bigArrays.newByteArray(startLen, randomBoolean());
        byte[] ref = new byte[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomByte();
            array = bigArrays.grow(array, i + 1);
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
        IntArray array = bigArrays.newIntArray(startLen, randomBoolean());
        int[] ref = new int[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomInt();
            array = bigArrays.grow(array, i + 1);
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
        LongArray array = bigArrays.newLongArray(startLen, randomBoolean());
        long[] ref = new long[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomLong();
            array = bigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(ref[i], array.get(i));
        }
        array.release();
    }

    public void testFloatArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        FloatArray array = bigArrays.newFloatArray(startLen, randomBoolean());
        float[] ref = new float[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomFloat();
            array = bigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(ref[i], array.get(i), 0.001d);
        }
        array.release();
    }

    public void testDoubleArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1000000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        DoubleArray array = bigArrays.newDoubleArray(startLen, randomBoolean());
        double[] ref = new double[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomDouble();
            array = bigArrays.grow(array, i + 1);
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
        ObjectArray<Object> array = bigArrays.newObjectArray(startLen);
        final Object[] pool = new Object[100];
        for (int i = 0; i < pool.length; ++i) {
            pool[i] = new Object();
        }
        Object[] ref = new Object[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            ref[i] = randomFrom(pool);
            array = bigArrays.grow(array, i + 1);
            array.set(i, ref[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertSame(ref[i], array.get(i));
        }
        array.release();
    }

    public void testByteArrayFill() {
        final int len = randomIntBetween(1, 100000);
        final int fromIndex = randomIntBetween(0, len - 1);
        final int toIndex = randomBoolean()
            ? Math.min(fromIndex + randomInt(100), len) // single page
            : randomIntBetween(fromIndex, len); // likely multiple pages
        final ByteArray array2 = bigArrays.newByteArray(len, randomBoolean());
        final byte[] array1 = new byte[len];
        for (int i = 0; i < len; ++i) {
            array1[i] = randomByte();
            array2.set(i, array1[i]);
        }
        final byte rand = randomByte();
        Arrays.fill(array1, fromIndex, toIndex, rand);
        array2.fill(fromIndex, toIndex, rand);
        for (int i = 0; i < len; ++i) {
            assertEquals(array1[i], array2.get(i), 0.001d);
        }
        array2.release();
    }

    public void testFloatArrayFill() {
        final int len = randomIntBetween(1, 100000);
        final int fromIndex = randomIntBetween(0, len - 1);
        final int toIndex = randomBoolean()
            ? Math.min(fromIndex + randomInt(100), len) // single page
            : randomIntBetween(fromIndex, len); // likely multiple pages
        final FloatArray array2 = bigArrays.newFloatArray(len, randomBoolean());
        final float[] array1 = new float[len];
        for (int i = 0; i < len; ++i) {
            array1[i] = randomFloat();
            array2.set(i, array1[i]);
        }
        final float rand = randomFloat();
        Arrays.fill(array1, fromIndex, toIndex, rand);
        array2.fill(fromIndex, toIndex, rand);
        for (int i = 0; i < len; ++i) {
            assertEquals(array1[i], array2.get(i), 0.001d);
        }
        array2.release();
    }

    public void testDoubleArrayFill() {
        final int len = randomIntBetween(1, 100000);
        final int fromIndex = randomIntBetween(0, len - 1);
        final int toIndex = randomBoolean()
            ? Math.min(fromIndex + randomInt(100), len) // single page
            : randomIntBetween(fromIndex, len); // likely multiple pages
        final DoubleArray array2 = bigArrays.newDoubleArray(len, randomBoolean());
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
        final LongArray array2 = bigArrays.newLongArray(len, randomBoolean());
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
        final ByteArray array2 = bigArrays.newByteArray(array1.length, randomBoolean());
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
        final ByteArray array2 = bigArrays.newByteArray(array1.length, randomBoolean());
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
