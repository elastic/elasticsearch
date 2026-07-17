/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class VectorReleasableTests extends ESTestCase {

    CircuitBreakerService breakerService;
    CircuitBreaker breaker;
    BlockFactory blockFactory;

    @Before
    public void initBlockFactory() throws Exception {
        breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
        breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        blockFactory = BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService)).build();
    }

    @After
    public void checkBreakerReleased() {
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testBooleanArrayVector() {
        var closed = new boolean[1];
        BooleanVector v = blockFactory.newBooleanArrayVector(new boolean[] { true, false }, 2);
        v.attachReleasable(() -> closed[0] = true);
        assertFalse(closed[0]);
        v.close();
        assertTrue(closed[0]);
    }

    public void testIntArrayVector() {
        var closed = new boolean[1];
        IntVector v = blockFactory.newIntArrayVector(new int[] { 1, 2, 3 }, 3);
        v.attachReleasable(() -> closed[0] = true);
        assertFalse(closed[0]);
        v.close();
        assertTrue(closed[0]);
    }

    public void testLongArrayVector() {
        var closed = new boolean[1];
        LongVector v = blockFactory.newLongArrayVector(new long[] { 1L, 2L }, 2);
        v.attachReleasable(() -> closed[0] = true);
        assertFalse(closed[0]);
        v.close();
        assertTrue(closed[0]);
    }

    public void testFloatArrayVector() {
        var closed = new boolean[1];
        FloatVector v = blockFactory.newFloatArrayVector(new float[] { 1.0f }, 1);
        v.attachReleasable(() -> closed[0] = true);
        assertFalse(closed[0]);
        v.close();
        assertTrue(closed[0]);
    }

    public void testDoubleArrayVector() {
        var closed = new boolean[1];
        DoubleVector v = blockFactory.newDoubleArrayVector(new double[] { 3.14 }, 1);
        v.attachReleasable(() -> closed[0] = true);
        assertFalse(closed[0]);
        v.close();
        assertTrue(closed[0]);
    }

    public void testBytesRefArrayVector() {
        var closed = new boolean[1];
        BytesRefArray array = new BytesRefArray(1, blockFactory.bigArrays());
        array.append(new BytesRef("hello"));
        BytesRefVector v = blockFactory.newBytesRefArrayVector(array, 1);
        v.attachReleasable(() -> closed[0] = true);
        assertFalse(closed[0]);
        v.close();
        assertTrue(closed[0]);
    }

    /** Callback fires when the wrapping block is closed, via the vector it holds. */
    public void testFiresWhenBlockReleased() {
        var closed = new boolean[1];
        IntVector v = blockFactory.newIntArrayVector(new int[] { 1 }, 1);
        v.attachReleasable(() -> closed[0] = true);
        Block block = v.asBlock();
        assertFalse(closed[0]);
        block.close();
        assertTrue(closed[0]);
    }

    /** Callback fires only on the final decRef, not on intermediate ones. */
    public void testFiresOnFinalDecRef() {
        var count = new AtomicInteger();
        IntVector v = blockFactory.newIntArrayVector(new int[] { 1 }, 1);
        v.attachReleasable(count::incrementAndGet);
        v.incRef();
        v.decRef();
        assertThat(count.get(), equalTo(0));
        v.decRef();
        assertThat(count.get(), equalTo(1));
    }

    /** BigArrayVector overrides closeInternal() without calling super — the hook must still fire. */
    public void testBigArrayVector() {
        BigArrays bigArrays = blockFactory.bigArrays();
        var closed = new boolean[1];
        IntArray array = bigArrays.newIntArray(3);
        array.set(0, 10);
        array.set(1, 20);
        array.set(2, 30);
        IntBigArrayVector v = new IntBigArrayVector(array, 3, blockFactory);
        v.attachReleasable(() -> closed[0] = true);
        assertFalse(closed[0]);
        v.close();
        assertTrue(closed[0]);
    }

    /** OrdinalBytesRefVector extends AbstractNonThreadSafeRefCounted directly — the hook must still fire. */
    public void testOrdinalBytesRefVector() {
        var closed = new boolean[1];
        IntVector ordinals = blockFactory.newIntArrayVector(new int[] { 0, 0, 1 }, 3);
        BytesRefArray bytesArray = new BytesRefArray(2, blockFactory.bigArrays());
        bytesArray.append(new BytesRef("foo"));
        bytesArray.append(new BytesRef("bar"));
        BytesRefVector bytes = blockFactory.newBytesRefArrayVector(bytesArray, 2);
        OrdinalBytesRefVector v = new OrdinalBytesRefVector(ordinals, bytes);
        v.attachReleasable(() -> closed[0] = true);
        assertFalse(closed[0]);
        v.close();
        assertTrue(closed[0]);
    }

    /** A second attachReleasable call must throw rather than silently overwrite the first. */
    public void testDoubleAttachThrows() {
        IntVector v = blockFactory.newIntArrayVector(new int[] { 1 }, 1);
        v.attachReleasable(() -> {});
        expectThrows(IllegalStateException.class, () -> v.attachReleasable(() -> {}));
        v.close();
    }

    /** Attaching after release must throw. */
    public void testAttachAfterReleaseThrows() {
        IntVector v = blockFactory.newIntArrayVector(new int[] { 1 }, 1);
        v.close();
        expectThrows(IllegalStateException.class, () -> v.attachReleasable(() -> {}));
    }

    /** Attaching null must throw. */
    public void testAttachNullThrows() {
        IntVector v = blockFactory.newIntArrayVector(new int[] { 1 }, 1);
        expectThrows(NullPointerException.class, () -> v.attachReleasable(null));
        v.close();
    }

    /** No callback attached: releasing a vector is fine. */
    public void testNoCallbackReleasesCleanly() {
        IntVector v = blockFactory.newIntArrayVector(new int[] { 1, 2 }, 2);
        v.close(); // must not throw
    }
}
