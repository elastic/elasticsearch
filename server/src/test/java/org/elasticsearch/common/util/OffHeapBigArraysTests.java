/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.offheap.OffHeapDecider;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class OffHeapBigArraysTests extends ESTestCase {

    private BigArrays randombigArrays() {
        Path tmpPath = createTempDir();
        return new MockOffHeapBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService(),
            "test", false, tmpPath, new OffHeapDecider() {
            @Override
            public Decision decide(long requireSizeInBytes) {
                return randomBoolean()? Decision.Mapped : Decision.Direct;
            }
        });
    }

    private BigArrays bigArrays;

    @Before
    public void init() {
        bigArrays = randombigArrays();
    }

    public void testIntArrayGrowth() {
        final int totalLen = randomIntBetween(1, 100000);
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
        final int totalLen = randomIntBetween(1, 100000);
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
                    Settings.builder()
                            .put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), maxSize, ByteSizeUnit.BYTES)
                            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                            .build(),
                    Collections.emptyList(),
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
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
        assertThat(size, greaterThanOrEqualTo((long)minSize));
        if (size >= pageSize) {
            assertThat(size + " is a multiple of " + pageSize, size % pageSize, equalTo(0L));
        }
        assertThat(size - minSize, lessThan((long) pageSize));
    }

    private List<BigArraysHelper> bigArrayCreators(final long maxSize, final boolean withBreaking) {
        final BigArrays byteBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper byteHelper = new BigArraysHelper(byteBigArrays,
            (Long size) -> byteBigArrays.newByteArray(size),
            (Long size) -> BigByteArray.estimateRamBytes(size));
        final BigArrays intBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper intHelper = new BigArraysHelper(intBigArrays,
            (Long size) -> intBigArrays.newIntArray(size),
            (Long size) -> BigIntArray.estimateRamBytes(size));
        final BigArrays longBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper longHelper = new BigArraysHelper(longBigArrays,
            (Long size) -> longBigArrays.newLongArray(size),
            (Long size) -> BigLongArray.estimateRamBytes(size));
        final BigArrays floatBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper floatHelper = new BigArraysHelper(floatBigArrays,
            (Long size) -> floatBigArrays.newFloatArray(size),
            (Long size) -> BigFloatArray.estimateRamBytes(size));
        final BigArrays doubleBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper doubleHelper = new BigArraysHelper(doubleBigArrays,
            (Long size) -> doubleBigArrays.newDoubleArray(size),
            (Long size) -> BigDoubleArray.estimateRamBytes(size));
        final BigArrays objectBigArrays = newBigArraysInstance(maxSize, withBreaking);
        BigArraysHelper objectHelper = new BigArraysHelper(objectBigArrays,
            (Long size) -> objectBigArrays.newObjectArray(size),
            (Long size) -> BigObjectArray.estimateRamBytes(size));
        return Arrays.asList(byteHelper, intHelper, longHelper, floatHelper, doubleHelper, objectHelper);
    }

    private BigArrays newBigArraysInstance(final long maxSize, final boolean withBreaking) {
        HierarchyCircuitBreakerService hcbs = new HierarchyCircuitBreakerService(
            Settings.builder()
                .put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), maxSize, ByteSizeUnit.BYTES)
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
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
