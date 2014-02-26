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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.*;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockBigArrays extends BigArrays {

    /**
     * Tracking allocations is useful when debugging a leak but shouldn't be enabled by default as this would also be very costly
     * since it creates a new Exception every time a new array is created.
     */
    private static final boolean TRACK_ALLOCATIONS = false;

    private static boolean DISCARD = false;

    private static ConcurrentMap<Object, Object> ACQUIRED_ARRAYS = new ConcurrentHashMap<Object, Object>();

    /**
     * Discard the next check that all arrays should be released. This can be useful if for a specific test, the cost to make
     * sure the array is released is higher than the cost the user would experience if the array would not be released.
     */
    public static void discardNextCheck() {
        DISCARD = true;
    }

    public static void ensureAllArraysAreReleased() {
        try {
            if (DISCARD) {
                DISCARD = false;
            } else if (ACQUIRED_ARRAYS.size() > 0) {
                final Object cause = ACQUIRED_ARRAYS.entrySet().iterator().next().getValue();
                throw new RuntimeException(ACQUIRED_ARRAYS.size() + " arrays have not been released", cause instanceof Throwable ? (Throwable) cause : null);
            }
        } finally {
            ACQUIRED_ARRAYS.clear();
        }
    }

    private final Random random;

    @Inject
    public MockBigArrays(Settings settings, PageCacheRecycler recycler) {
        super(settings, recycler);
        long seed;
        try {
            seed = SeedUtils.parseSeed(RandomizedContext.current().getRunnerSeedAsString());
        } catch (IllegalStateException e) { // rest tests don't run randomized and have no context
            seed = 0;
        }
        random = new Random(seed);
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
        arr.in = super.resize(arr.in, size);
        if (arr.in instanceof ByteArrayWrapper) {
            ACQUIRED_ARRAYS.remove(arr);
            arr = (ByteArrayWrapper) arr.in;
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
        arr.in = super.resize(arr.in, size);
        if (arr.in instanceof IntArrayWrapper) {
            ACQUIRED_ARRAYS.remove(arr);
            arr = (IntArrayWrapper) arr.in;
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
        arr.in = super.resize(arr.in, size);
        if (arr.in instanceof LongArrayWrapper) {
            ACQUIRED_ARRAYS.remove(arr);
            arr = (LongArrayWrapper) arr.in;
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
        arr.in = super.resize(arr.in, size);
        if (arr.in instanceof DoubleArrayWrapper) {
            ACQUIRED_ARRAYS.remove(arr);
            arr = (DoubleArrayWrapper) arr.in;
        }
        if (!arr.clearOnResize) {
            arr.randomizeContent(originalSize, size);
        }
        return arr;
    }

    @Override
    public <T> ObjectArray<T> newObjectArray(long size) {
        return new ObjectArrayWrapper<T>(super.<T>newObjectArray(size));
    }

    @Override
    public <T> ObjectArray<T> resize(ObjectArray<T> array, long size) {
        ObjectArrayWrapper<T> arr = (ObjectArrayWrapper<T>) array;
        arr.in = super.resize(arr.in, size);
        if (arr.in instanceof ObjectArrayWrapper) {
            ACQUIRED_ARRAYS.remove(arr);
            arr = (ObjectArrayWrapper<T>) arr.in;
        }
        return arr;
    }

    private static abstract class AbstractArrayWrapper {

        boolean clearOnResize;
        boolean released = false;

        AbstractArrayWrapper(boolean clearOnResize) {
            ACQUIRED_ARRAYS.put(this, TRACK_ALLOCATIONS ? new RuntimeException() : Boolean.TRUE);
            this.clearOnResize = clearOnResize;
        }

        protected abstract BigArray getDelegate();

        protected abstract void randomizeContent(long from, long to);

        public long size() {
            return getDelegate().size();
        }

        public boolean release() {
            assert !released;
            ACQUIRED_ARRAYS.remove(this);
            randomizeContent(0, size());
            released = true;
            return getDelegate().release();
        }

    }

    private class ByteArrayWrapper extends AbstractArrayWrapper implements ByteArray {

        private ByteArray in;

        ByteArrayWrapper(ByteArray in, boolean clearOnResize) {
            super(clearOnResize);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        protected void randomizeContent(long from, long to) {
            for (long i = from; i < to; ++i) {
                set(i, (byte) random.nextInt(1 << 8));
            }
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
        public void get(long index, int len, BytesRef ref) {
            in.get(index, len, ref);
        }

        @Override
        public void set(long index, byte[] buf, int offset, int len) {
            in.set(index, buf, offset, len);
        }

    }

    private class IntArrayWrapper extends AbstractArrayWrapper implements IntArray {

        private IntArray in;

        IntArrayWrapper(IntArray in, boolean clearOnResize) {
            super(clearOnResize);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        protected void randomizeContent(long from, long to) {
            for (long i = from; i < to; ++i) {
                set(i, random.nextInt());
            }
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

    }

    private class LongArrayWrapper extends AbstractArrayWrapper implements LongArray {

        private LongArray in;

        LongArrayWrapper(LongArray in, boolean clearOnResize) {
            super(clearOnResize);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        protected void randomizeContent(long from, long to) {
            for (long i = from; i < to; ++i) {
                set(i, random.nextLong());
            }
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

    private class DoubleArrayWrapper extends AbstractArrayWrapper implements DoubleArray {

        private DoubleArray in;

        DoubleArrayWrapper(DoubleArray in, boolean clearOnResize) {
            super(clearOnResize);
            this.in = in;
        }

        @Override
        protected BigArray getDelegate() {
            return in;
        }

        @Override
        protected void randomizeContent(long from, long to) {
            for (long i = from; i < to; ++i) {
                set(i, (random.nextDouble() - 0.5) * 1000);
            }
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

        private ObjectArray<T> in;

        ObjectArrayWrapper(ObjectArray<T> in) {
            super(false);
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
