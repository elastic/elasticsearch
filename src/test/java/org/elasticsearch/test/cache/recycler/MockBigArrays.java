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

package org.elasticsearch.test.cache.recycler;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.*;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class MockBigArrays extends BigArrays {

    /**
     * Tracking allocations is useful when debugging a leak but shouldn't be enabled by default as this would also be very costly
     * since it creates a new Exception every time a new array is created.
     */
    private static final boolean TRACK_ALLOCATIONS = false;

    private static final Set<BigArrays> INSTANCES = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<BigArrays, Boolean>()));
    private static final ConcurrentMap<Object, Object> ACQUIRED_ARRAYS = new ConcurrentHashMap<>();

    public static void ensureAllArraysAreReleased() throws Exception {
        final Map<Object, Object> masterCopy = Maps.newHashMap(ACQUIRED_ARRAYS);
        if (!masterCopy.isEmpty()) {
            // not empty, we might be executing on a shared cluster that keeps on obtaining
            // and releasing arrays, lets make sure that after a reasonable timeout, all master
            // copy (snapshot) have been released
            boolean success = ElasticsearchTestCase.awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object input) {
                    return Sets.intersection(masterCopy.keySet(), ACQUIRED_ARRAYS.keySet()).isEmpty();
                }
            });
            if (!success) {
                masterCopy.keySet().retainAll(ACQUIRED_ARRAYS.keySet());
                ACQUIRED_ARRAYS.keySet().removeAll(masterCopy.keySet()); // remove all existing master copy we will report on
                if (!masterCopy.isEmpty()) {
                    final Object cause = masterCopy.entrySet().iterator().next().getValue();
                    throw new RuntimeException(masterCopy.size() + " arrays have not been released", cause instanceof Throwable ? (Throwable) cause : null);
                }
            }
        }
    }

    private final Random random;
    private final Settings settings;
    private final PageCacheRecycler recycler;
    private final CircuitBreakerService breakerService;

    @Inject
    public MockBigArrays(Settings settings, PageCacheRecycler recycler, CircuitBreakerService breakerService) {
        this(settings, recycler, breakerService, false);
    }

    public MockBigArrays(Settings settings, PageCacheRecycler recycler, CircuitBreakerService breakerService, boolean checkBreaker) {
        super(settings, recycler, breakerService, checkBreaker);
        this.settings = settings;
        this.recycler = recycler;
        this.breakerService = breakerService;
        long seed;
        try {
            seed = SeedUtils.parseSeed(RandomizedContext.current().getRunnerSeedAsString());
        } catch (IllegalStateException e) { // rest tests don't run randomized and have no context
            seed = 0;
        }
        random = new Random(seed);
        INSTANCES.add(this);
    }


    @Override
    public BigArrays withCircuitBreaking() {
        return new MockBigArrays(this.settings, this.recycler, this.breakerService, true);
    }

    @Override
    public ByteArray newByteArray(long size, boolean clearOnResize) {
        final ByteArrayWrapper array = new ByteArrayWrapper(super.newByteArray(size, clearOnResize), clearOnResize);
        if (!clearOnResize) {
            array.randomizeContent(0, size);
        }
        return array;
    }

    @Override
    public ByteArray resize(ByteArray array, long size) {
        ByteArrayWrapper arr = (ByteArrayWrapper) array;
        final long originalSize = arr.size();
        array = super.resize(arr.in, size);
        ACQUIRED_ARRAYS.remove(arr);
        if (array instanceof ByteArrayWrapper) {
            arr = (ByteArrayWrapper) array;
        } else {
            arr = new ByteArrayWrapper(array, arr.clearOnResize);
        }
        if (!arr.clearOnResize) {
            arr.randomizeContent(originalSize, size);
        }
        return arr;
    }

    @Override
    public IntArray newIntArray(long size, boolean clearOnResize) {
        final IntArrayWrapper array = new IntArrayWrapper(super.newIntArray(size, clearOnResize), clearOnResize);
        if (!clearOnResize) {
            array.randomizeContent(0, size);
        }
        return array;
    }

    @Override
    public IntArray resize(IntArray array, long size) {
        IntArrayWrapper arr = (IntArrayWrapper) array;
        final long originalSize = arr.size();
        array = super.resize(arr.in, size);
        ACQUIRED_ARRAYS.remove(arr);
        if (array instanceof IntArrayWrapper) {
            arr = (IntArrayWrapper) array;
        } else {
            arr = new IntArrayWrapper(array, arr.clearOnResize);
        }
        if (!arr.clearOnResize) {
            arr.randomizeContent(originalSize, size);
        }
        return arr;
    }

    @Override
    public LongArray newLongArray(long size, boolean clearOnResize) {
        final LongArrayWrapper array = new LongArrayWrapper(super.newLongArray(size, clearOnResize), clearOnResize);
        if (!clearOnResize) {
            array.randomizeContent(0, size);
        }
        return array;
    }

    @Override
    public LongArray resize(LongArray array, long size) {
        LongArrayWrapper arr = (LongArrayWrapper) array;
        final long originalSize = arr.size();
        array = super.resize(arr.in, size);
        ACQUIRED_ARRAYS.remove(arr);
        if (array instanceof LongArrayWrapper) {
            arr = (LongArrayWrapper) array;
        } else {
            arr = new LongArrayWrapper(array, arr.clearOnResize);
        }
        if (!arr.clearOnResize) {
            arr.randomizeContent(originalSize, size);
        }
        return arr;
    }

    @Override
    public FloatArray newFloatArray(long size, boolean clearOnResize) {
        final FloatArrayWrapper array = new FloatArrayWrapper(super.newFloatArray(size, clearOnResize), clearOnResize);
        if (!clearOnResize) {
            array.randomizeContent(0, size);
        }
        return array;
    }

    @Override
    public FloatArray resize(FloatArray array, long size) {
        FloatArrayWrapper arr = (FloatArrayWrapper) array;
        final long originalSize = arr.size();
        array = super.resize(arr.in, size);
        ACQUIRED_ARRAYS.remove(arr);
        if (array instanceof FloatArrayWrapper) {
            arr = (FloatArrayWrapper) array;
        } else {
            arr = new FloatArrayWrapper(array, arr.clearOnResize);
        }
        if (!arr.clearOnResize) {
            arr.randomizeContent(originalSize, size);
        }
        return arr;
    }

    @Override
    public DoubleArray newDoubleArray(long size, boolean clearOnResize) {
        final DoubleArrayWrapper array = new DoubleArrayWrapper(super.newDoubleArray(size, clearOnResize), clearOnResize);
        if (!clearOnResize) {
            array.randomizeContent(0, size);
        }
        return array;
    }

    @Override
    public DoubleArray resize(DoubleArray array, long size) {
        DoubleArrayWrapper arr = (DoubleArrayWrapper) array;
        final long originalSize = arr.size();
        array = super.resize(arr.in, size);
        ACQUIRED_ARRAYS.remove(arr);
        if (array instanceof DoubleArrayWrapper) {
            arr = (DoubleArrayWrapper) array;
        } else {
            arr = new DoubleArrayWrapper(array, arr.clearOnResize);
        }
        if (!arr.clearOnResize) {
            arr.randomizeContent(originalSize, size);
        }
        return arr;
    }

    @Override
    public <T> ObjectArray<T> newObjectArray(long size) {
        return new ObjectArrayWrapper<>(super.<T>newObjectArray(size));
    }

    @Override
    public <T> ObjectArray<T> resize(ObjectArray<T> array, long size) {
        ObjectArrayWrapper<T> arr = (ObjectArrayWrapper<T>) array;
        array = super.resize(arr.in, size);
        ACQUIRED_ARRAYS.remove(arr);
        if (array instanceof ObjectArrayWrapper) {
            arr = (ObjectArrayWrapper<T>) array;
        } else {
            arr = new ObjectArrayWrapper<>(array);
        }
        return arr;
    }

    private static abstract class AbstractArrayWrapper {

        final BigArray in;
        boolean clearOnResize;
        AtomicBoolean released;

        AbstractArrayWrapper(BigArray in, boolean clearOnResize) {
            ACQUIRED_ARRAYS.put(this, TRACK_ALLOCATIONS ? new RuntimeException() : Boolean.TRUE);
            this.in = in;
            this.clearOnResize = clearOnResize;
            released = new AtomicBoolean(false);
        }

        protected abstract BigArray getDelegate();

        protected abstract void randomizeContent(long from, long to);

        public long size() {
            return getDelegate().size();
        }

        public long ramBytesUsed() {
            return in.ramBytesUsed();
        }

        public void close() {
            if (!released.compareAndSet(false, true)) {
                throw new IllegalStateException("Double release");
            }
            ACQUIRED_ARRAYS.remove(this);
            randomizeContent(0, size());
            getDelegate().close();
        }

    }

    private class ByteArrayWrapper extends AbstractArrayWrapper implements ByteArray {

        private final ByteArray in;

        ByteArrayWrapper(ByteArray in, boolean clearOnResize) {
            super(in, clearOnResize);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        protected void randomizeContent(long from, long to) {
            fill(from, to, (byte) random.nextInt(1 << 8));
        }

        @Override
        public byte get(long index) {
            return in.get(index);
        }

        @Override
        public byte set(long index, byte value) {
            return in.set(index, value);
        }

        @Override
        public boolean get(long index, int len, BytesRef ref) {
            return in.get(index, len, ref);
        }

        @Override
        public void set(long index, byte[] buf, int offset, int len) {
            in.set(index, buf, offset, len);
        }

        @Override
        public void fill(long fromIndex, long toIndex, byte value) {
            in.fill(fromIndex, toIndex, value);
        }

    }

    private class IntArrayWrapper extends AbstractArrayWrapper implements IntArray {

        private final IntArray in;

        IntArrayWrapper(IntArray in, boolean clearOnResize) {
            super(in, clearOnResize);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        protected void randomizeContent(long from, long to) {
            fill(from, to, random.nextInt());
        }

        @Override
        public int get(long index) {
            return in.get(index);
        }

        @Override
        public int set(long index, int value) {
            return in.set(index, value);
        }

        @Override
        public int increment(long index, int inc) {
            return in.increment(index, inc);
        }

        @Override
        public void fill(long fromIndex, long toIndex, int value) {
            in.fill(fromIndex, toIndex, value);
        }

    }

    private class LongArrayWrapper extends AbstractArrayWrapper implements LongArray {

        private final LongArray in;

        LongArrayWrapper(LongArray in, boolean clearOnResize) {
            super(in, clearOnResize);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        protected void randomizeContent(long from, long to) {
            fill(from, to, random.nextLong());
        }

        @Override
        public long get(long index) {
            return in.get(index);
        }

        @Override
        public long set(long index, long value) {
            return in.set(index, value);
        }

        @Override
        public long increment(long index, long inc) {
            return in.increment(index, inc);
        }

        @Override
        public void fill(long fromIndex, long toIndex, long value) {
            in.fill(fromIndex, toIndex, value);
        }

    }

    private class FloatArrayWrapper extends AbstractArrayWrapper implements FloatArray {

        private final FloatArray in;

        FloatArrayWrapper(FloatArray in, boolean clearOnResize) {
            super(in, clearOnResize);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        protected void randomizeContent(long from, long to) {
            fill(from, to, (random.nextFloat() - 0.5f) * 1000);
        }

        @Override
        public float get(long index) {
            return in.get(index);
        }

        @Override
        public float set(long index, float value) {
            return in.set(index, value);
        }

        @Override
        public float increment(long index, float inc) {
            return in.increment(index, inc);
        }

        @Override
        public void fill(long fromIndex, long toIndex, float value) {
            in.fill(fromIndex, toIndex, value);
        }

    }

    private class DoubleArrayWrapper extends AbstractArrayWrapper implements DoubleArray {

        private final DoubleArray in;

        DoubleArrayWrapper(DoubleArray in, boolean clearOnResize) {
            super(in, clearOnResize);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        protected void randomizeContent(long from, long to) {
            fill(from, to, (random.nextDouble() - 0.5) * 1000);
        }

        @Override
        public double get(long index) {
            return in.get(index);
        }

        @Override
        public double set(long index, double value) {
            return in.set(index, value);
        }

        @Override
        public double increment(long index, double inc) {
            return in.increment(index, inc);
        }

        @Override
        public void fill(long fromIndex, long toIndex, double value) {
            in.fill(fromIndex, toIndex, value);
        }

    }

    private class ObjectArrayWrapper<T> extends AbstractArrayWrapper implements ObjectArray<T> {

        private final ObjectArray<T> in;

        ObjectArrayWrapper(ObjectArray<T> in) {
            super(in, false);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        public T get(long index) {
            return in.get(index);
        }

        @Override
        public T set(long index, T value) {
            return in.set(index, value);
        }

        @Override
        protected void randomizeContent(long from, long to) {
            // will be cleared anyway
        }

    }

}
