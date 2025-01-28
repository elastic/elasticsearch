/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.PreallocatedCircuitBreakerService;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BigArraysTests extends ESTestCase {
    private final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

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
        array.close();
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
        array.close();
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
        array.close();
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
        array.close();
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
        array.close();
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
            assertEquals(ref[i], array.getAndSet(i, ref[i]));
        }
        for (int i = 0; i < totalLen; ++i) {
            assertSame(ref[i], array.get(i));
        }
        array.close();
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
        array2.close();
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
        array2.close();
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
        array2.close();
    }

    public void testIntArrayFill() {
        final int len = randomIntBetween(1, 100000);
        final int fromIndex = randomIntBetween(0, len - 1);
        final int toIndex = randomBoolean()
            ? Math.min(fromIndex + randomInt(100), len) // single page
            : randomIntBetween(fromIndex, len); // likely multiple pages
        final IntArray array2 = bigArrays.newIntArray(len, randomBoolean());
        final int[] array1 = new int[len];
        for (int i = 0; i < len; ++i) {
            array1[i] = randomInt();
            array2.set(i, array1[i]);
        }
        final int rand = randomInt();
        Arrays.fill(array1, fromIndex, toIndex, rand);
        array2.fill(fromIndex, toIndex, rand);
        for (int i = 0; i < len; ++i) {
            assertEquals(array1[i], array2.get(i));
        }
        array2.close();
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
        array2.close();
    }

    public void testSerializeDoubleArray() throws Exception {
        int len = randomIntBetween(1, 100_000);
        DoubleArray array1 = bigArrays.newDoubleArray(len, randomBoolean());
        for (int i = 0; i < len; ++i) {
            array1.set(i, randomDouble());
        }
        if (randomBoolean()) {
            len = randomIntBetween(len, len * 3 / 2);
            array1 = bigArrays.resize(array1, len);
        }
        BytesStreamOutput out = new BytesStreamOutput();
        array1.writeTo(out);
        final DoubleArray array2 = bigArrays.newDoubleArray(len, randomBoolean());
        array2.fillWith(out.bytes().streamInput());
        for (int i = 0; i < len; i++) {
            assertThat(array2.get(i), equalTo(array1.get(i)));
        }
        final DoubleArray array3 = DoubleArray.readFrom(out.bytes().streamInput());
        assertThat(array3.size(), equalTo((long) len));
        for (int i = 0; i < len; i++) {
            assertThat(array3.get(i), equalTo(array1.get(i)));
        }
        Releasables.close(array1, array2, array3);
    }

    public void testSerializeLongArray() throws Exception {
        int len = randomIntBetween(1, 100_000);
        LongArray array1 = bigArrays.newLongArray(len, randomBoolean());
        for (int i = 0; i < len; ++i) {
            array1.set(i, randomLong());
        }
        if (randomBoolean()) {
            len = randomIntBetween(len, len * 3 / 2);
            array1 = bigArrays.resize(array1, len);
        }
        BytesStreamOutput out = new BytesStreamOutput();
        array1.writeTo(out);
        final LongArray array2 = bigArrays.newLongArray(len, randomBoolean());
        array2.fillWith(out.bytes().streamInput());
        for (int i = 0; i < len; i++) {
            assertThat(array2.get(i), equalTo(array1.get(i)));
        }
        final LongArray array3 = LongArray.readFrom(out.bytes().streamInput());
        assertThat(array3.size(), equalTo((long) len));
        for (int i = 0; i < len; i++) {
            assertThat(array3.get(i), equalTo(array1.get(i)));
        }
        Releasables.close(array1, array2, array3);
    }

    public void testSerializeIntArray() throws Exception {
        int len = randomIntBetween(1, 100_000);
        IntArray array1 = bigArrays.newIntArray(len, randomBoolean());
        for (int i = 0; i < len; ++i) {
            array1.set(i, randomInt());
        }
        if (randomBoolean()) {
            len = randomIntBetween(len, len * 3 / 2);
            array1 = bigArrays.resize(array1, len);
        }
        BytesStreamOutput out = new BytesStreamOutput();
        array1.writeTo(out);
        final IntArray array2 = bigArrays.newIntArray(len, randomBoolean());
        array2.fillWith(out.bytes().streamInput());
        for (int i = 0; i < len; i++) {
            assertThat(array2.get(i), equalTo(array1.get(i)));
        }
        final IntArray array3 = IntArray.readFrom(out.bytes().streamInput());
        assertThat(array3.size(), equalTo((long) len));
        for (int i = 0; i < len; i++) {
            assertThat(array3.get(i), equalTo(array1.get(i)));
        }
        Releasables.close(array1, array2, array3);
    }

    public void testByteArrayBulkGet() {
        final byte[] array1 = new byte[randomIntBetween(1, 4000000)];
        random().nextBytes(array1);
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
        array2.close();
    }

    public void testByteArrayBulkSet() {
        final byte[] array1 = new byte[randomIntBetween(1, 4000000)];
        random().nextBytes(array1);
        final ByteArray array2 = bigArrays.newByteArray(array1.length, randomBoolean());
        for (int i = 0; i < array1.length;) {
            final int len = Math.min(array1.length - i, randomBoolean() ? randomInt(10) : randomInt(3 * PageCacheRecycler.BYTE_PAGE_SIZE));
            array2.set(i, array1, i, len);
            i += len;
        }
        for (int i = 0; i < array1.length; ++i) {
            assertEquals(array1[i], array2.get(i));
        }
        array2.close();
    }

    public void testByteIterator() throws Exception {
        final byte[] bytes = new byte[randomIntBetween(1, 4000000)];
        random().nextBytes(bytes);
        ByteArray array = bigArrays.newByteArray(bytes.length, randomBoolean());
        array.fillWith(new ByteArrayStreamInput(bytes));
        for (int i = 0; i < bytes.length; i++) {
            assertEquals(bytes[i], array.get(i));
        }
        BytesRefIterator it = array.iterator();
        BytesRef ref;
        int offset = 0;
        while ((ref = it.next()) != null) {
            for (int i = 0; i < ref.length; i++) {
                assertEquals(bytes[offset], ref.bytes[ref.offset + i]);
                offset++;
            }
        }
        assertThat(offset, equalTo(bytes.length));
        int newLen = randomIntBetween(bytes.length, bytes.length + 100_000);
        array = bigArrays.resize(array, newLen);
        it = array.iterator();
        offset = 0;
        while ((ref = it.next()) != null) {
            for (int i = 0; i < ref.length; i++) {
                if (offset < bytes.length) {
                    assertEquals(bytes[offset], ref.bytes[ref.offset + i]);
                }
                offset++;
            }
        }
        assertThat(offset, equalTo(newLen));
        array.close();
    }

    public void testByteArrayEquals() {
        final ByteArray empty1 = byteArrayWithBytes(BytesRef.EMPTY_BYTES);
        final ByteArray empty2 = byteArrayWithBytes(BytesRef.EMPTY_BYTES);

        // identity = equality
        assertTrue(BigArrays.equals(empty1, empty1));
        // equality: both empty
        assertTrue(BigArrays.equals(empty1, empty2));
        empty1.close();
        empty2.close();

        // not equal: contents differ
        final ByteArray a1 = byteArrayWithBytes(new byte[] { 0 });
        final ByteArray a2 = byteArrayWithBytes(new byte[] { 1 });
        assertFalse(BigArrays.equals(a1, a2));
        a1.close();
        a2.close();

        // not equal: contents differ
        final ByteArray a3 = byteArrayWithBytes(new byte[] { 1, 2, 3 });
        final ByteArray a4 = byteArrayWithBytes(new byte[] { 1, 1, 3 });
        assertFalse(BigArrays.equals(a3, a4));
        a3.close();
        a4.close();

        // not equal: contents differ
        final ByteArray a5 = byteArrayWithBytes(new byte[] { 1, 2, 3 });
        final ByteArray a6 = byteArrayWithBytes(new byte[] { 1, 2, 4 });
        assertFalse(BigArrays.equals(a5, a6));
        a5.close();
        a6.close();
    }

    public void testByteArrayHashCode() {
        // null arg has hashCode 0
        assertEquals(0, BigArrays.hashCode(null));

        // empty array should have equal hash
        final int emptyHash = Arrays.hashCode(BytesRef.EMPTY_BYTES);
        final ByteArray emptyByteArray = byteArrayWithBytes(BytesRef.EMPTY_BYTES);
        final int emptyByteArrayHash = BigArrays.hashCode(emptyByteArray);
        assertEquals(emptyHash, emptyByteArrayHash);
        emptyByteArray.close();

        // FUN FACT: Arrays.hashCode() and BytesReference.bytesHashCode() are inconsistent for empty byte[]
        // final int emptyHash3 = new BytesArray(BytesRef.EMPTY_BYTES).hashCode();
        // assertEquals(emptyHash1, emptyHash3); -> fail (1 vs. 0)

        // large arrays should be different
        final byte[] array1 = new byte[randomIntBetween(1, 4000000)];
        random().nextBytes(array1);
        final int array1Hash = Arrays.hashCode(array1);
        final ByteArray array2 = byteArrayWithBytes(array1);
        final int array2Hash = BigArrays.hashCode(array2);
        assertEquals(array1Hash, array2Hash);
        array2.close();
    }

    private ByteArray byteArrayWithBytes(byte[] bytes) {
        ByteArray bytearray = bigArrays.newByteArray(bytes.length);
        for (int i = 0; i < bytes.length; ++i) {
            bytearray.set(i, bytes[i]);
        }
        return bytearray;
    }

    public void testMaxSizeExceededOnNew() throws Exception {
        final long size = scaledRandomIntBetween(5, 1 << 22);
        final long maxSize = size - 1;
        for (BigArraysHelper bigArraysHelper : bigArrayCreators(maxSize, true)) {
            try {
                bigArraysHelper.arrayAllocator.apply(size);
                fail("circuit breaker should trip");
            } catch (CircuitBreakingException e) {
                assertEquals(maxSize, e.getByteLimit());
                assertThat(e.getBytesWanted(), greaterThanOrEqualTo(size));
            }
            assertEquals(0, bigArraysHelper.bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed());
        }
    }

    public void testMaxSizeExceededOnResize() throws Exception {
        for (String type : Arrays.asList("Byte", "Int", "Long", "Float", "Double", "Object")) {
            final int maxSize = randomIntBetween(1 << 8, 1 << 14);
            HierarchyCircuitBreakerService hcbs = new HierarchyCircuitBreakerService(
                CircuitBreakerMetrics.NOOP,
                Settings.builder()
                    .put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), maxSize, ByteSizeUnit.BYTES)
                    .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                    .build(),
                Collections.emptyList(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            BigArrays bigArrays = new BigArrays(null, hcbs, CircuitBreaker.REQUEST).withCircuitBreaking();
            Method create = BigArrays.class.getMethod("new" + type + "Array", long.class);
            final int size = scaledRandomIntBetween(10, maxSize / 16);
            BigArray array = (BigArray) create.invoke(bigArrays, size);
            Method resize = BigArrays.class.getMethod("resize", array.getClass().getInterfaces()[0], long.class);
            while (true) {
                long newSize = array.size() * 2;
                try {
                    array = (BigArray) resize.invoke(bigArrays, array, newSize);
                } catch (InvocationTargetException e) {
                    assertTrue(e.getCause() instanceof CircuitBreakingException);
                    break;
                }
            }
            assertEquals(array.ramBytesUsed(), hcbs.getBreaker(CircuitBreaker.REQUEST).getUsed());
            array.close();
            assertEquals(0, hcbs.getBreaker(CircuitBreaker.REQUEST).getUsed());
        }
    }

    public void testEstimatedBytesSameAsActualBytes() throws Exception {
        final int maxSize = 1 << scaledRandomIntBetween(15, 22);
        final long size = randomIntBetween((1 << 14) + 1, maxSize);
        for (final BigArraysHelper bigArraysHelper : bigArrayCreators(maxSize, false)) {
            final BigArray bigArray = bigArraysHelper.arrayAllocator.apply(size);
            assertEquals(bigArraysHelper.ramEstimator.apply(size).longValue(), bigArray.ramBytesUsed());
        }
    }

    public void testOverSizeUsesMinPageCount() {
        final int pageSize = 1 << (randomIntBetween(2, 16));
        final int minSize = randomIntBetween(1, pageSize) * randomIntBetween(1, 100);
        final long size = BigArrays.overSize(minSize, pageSize, 1);
        assertThat(size, greaterThanOrEqualTo((long) minSize));
        if (size >= pageSize) {
            assertThat(size + " is a multiple of " + pageSize, size % pageSize, equalTo(0L));
        }
        assertThat(size - minSize, lessThan((long) pageSize));
    }

    /**
     * Test the pattern we use to pre-allocate space for many {@link BigArray}s.
     */
    public void testPreallocate() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        HierarchyCircuitBreakerService realBreakers = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            List.of(),
            clusterSettings
        );
        BigArrays unPreAllocated = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), realBreakers);
        long toPreallocate = randomLongBetween(4000, 10000);
        CircuitBreaker realBreaker = realBreakers.getBreaker(CircuitBreaker.REQUEST);
        assertThat(realBreaker.getUsed(), equalTo(0L));
        try (
            PreallocatedCircuitBreakerService prealloctedBreakerService = new PreallocatedCircuitBreakerService(
                realBreakers,
                CircuitBreaker.REQUEST,
                toPreallocate,
                "test"
            )
        ) {
            assertThat(realBreaker.getUsed(), equalTo(toPreallocate));
            BigArrays preallocated = unPreAllocated.withBreakerService(prealloctedBreakerService);

            // We don't grab any bytes just making a new BigArrays
            assertThat(realBreaker.getUsed(), equalTo(toPreallocate));

            List<BigArray> arrays = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                // We're well under the preallocation so grabbing a little array doesn't allocate anything
                arrays.add(preallocated.newLongArray(1));
                assertThat(realBreaker.getUsed(), equalTo(toPreallocate));
            }

            // Allocating a large array *does* allocate some bytes
            arrays.add(preallocated.newLongArray(1024));
            long expectedMin = (PageCacheRecycler.LONG_PAGE_SIZE + arrays.size()) * Long.BYTES;
            assertThat(realBreaker.getUsed(), greaterThanOrEqualTo(expectedMin));
            // 64 should be enough room for each BigArray object
            assertThat(realBreaker.getUsed(), lessThanOrEqualTo(expectedMin + 64 * arrays.size()));
            Releasables.close(arrays);
        }
        assertThat(realBreaker.getUsed(), equalTo(0L));
    }

    private List<BigArraysHelper> bigArrayCreators(final long maxSize, final boolean withBreaking) {
        final BigArrays byteBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper byteHelper = new BigArraysHelper(
            byteBigArrays,
            (Long size) -> byteBigArrays.newByteArray(size),
            (Long size) -> BigByteArray.estimateRamBytes(size)
        );
        final BigArrays intBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper intHelper = new BigArraysHelper(
            intBigArrays,
            (Long size) -> intBigArrays.newIntArray(size),
            (Long size) -> BigIntArray.estimateRamBytes(size)
        );
        final BigArrays longBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper longHelper = new BigArraysHelper(
            longBigArrays,
            (Long size) -> longBigArrays.newLongArray(size),
            (Long size) -> BigLongArray.estimateRamBytes(size)
        );
        final BigArrays floatBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper floatHelper = new BigArraysHelper(
            floatBigArrays,
            (Long size) -> floatBigArrays.newFloatArray(size),
            (Long size) -> BigFloatArray.estimateRamBytes(size)
        );
        final BigArrays doubleBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper doubleHelper = new BigArraysHelper(
            doubleBigArrays,
            (Long size) -> doubleBigArrays.newDoubleArray(size),
            (Long size) -> BigDoubleArray.estimateRamBytes(size)
        );
        final BigArrays objectBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper objectHelper = new BigArraysHelper(
            objectBigArrays,
            (Long size) -> objectBigArrays.newObjectArray(size),
            (Long size) -> BigObjectArray.estimateRamBytes(size)
        );
        return Arrays.asList(byteHelper, intHelper, longHelper, floatHelper, doubleHelper, objectHelper);
    }

    private BigArrays newBigArraysInstance(final long maxSize, final boolean withBreaking) {
        HierarchyCircuitBreakerService hcbs = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.builder()
                .put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), maxSize, ByteSizeUnit.BYTES)
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        BigArrays bigArrays = new BigArrays(null, hcbs, CircuitBreaker.REQUEST);
        return (withBreaking ? bigArrays.withCircuitBreaking() : bigArrays);
    }

    private static class BigArraysHelper {
        final BigArrays bigArrays;
        final Function<Long, BigArray> arrayAllocator;
        final Function<Long, Long> ramEstimator;

        BigArraysHelper(BigArrays bigArrays, Function<Long, BigArray> arrayAllocator, Function<Long, Long> ramEstimator) {
            this.bigArrays = bigArrays;
            this.arrayAllocator = arrayAllocator;
            this.ramEstimator = ramEstimator;
        }
    }

}
