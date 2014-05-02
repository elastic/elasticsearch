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

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link BigArrays} impl that tracks the total number of allocated bytes.
 */
public class ByteTrackingBigArrays extends BigArrays {

    private static final long PER_INSTANCE_OVERHEAD = RamUsageEstimator.shallowSizeOfInstance(ByteArrayWrapper.class) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

    private final BigArrays in;
    private final long limit;
    private AtomicInteger allocatedInstances;
    private AtomicLong allocatedBytes;

    public ByteTrackingBigArrays(BigArrays in, long limit) {
        super(ImmutableSettings.EMPTY);
        this.in = in;
        this.limit = limit;
        allocatedInstances = new AtomicInteger();
        allocatedBytes = new AtomicLong();
    }

    private long ramBytesAllocated(int instances, long bytes) {
        return PER_INSTANCE_OVERHEAD * instances + bytes;
    }

    private void checkRamBytesAllocated(Releasable releasable, int instances, long bytes) {
        final long memoryUsage = ramBytesAllocated(instances, bytes);
        if (memoryUsage >= limit) {
            Releasables.closeWhileHandlingException(releasable);
            throw new ElasticsearchIllegalStateException("Query tried to allocate more memory than it is allowed to: " + RamUsageEstimator.humanReadableUnits(limit));
        }
    }

    private void onNewInstance(Releasable releasable, long bytes) {
        checkRamBytesAllocated(releasable, allocatedInstances.incrementAndGet(), allocatedBytes.addAndGet(bytes));
    }

    private void onResize(Releasable releasable, long bytesDelta) {
        checkRamBytesAllocated(releasable, allocatedInstances.get(), allocatedBytes.addAndGet(bytesDelta));
    }

    @Override
    public ByteArray newByteArray(long size, boolean clearOnResize) {
        final ByteArray array = in.newByteArray(size, clearOnResize);
        onNewInstance(array, array.size() * RamUsageEstimator.NUM_BYTES_BYTE);
        return array;
    }

    @Override
    public IntArray newIntArray(long size, boolean clearOnResize) {
        final IntArray array = in.newIntArray(size, clearOnResize);
        onNewInstance(array, array.size() * RamUsageEstimator.NUM_BYTES_INT);
        return array;
    }

    @Override
    public LongArray newLongArray(long size, boolean clearOnResize) {
        final LongArray array = in.newLongArray(size, clearOnResize);
        onNewInstance(array, array.size() * RamUsageEstimator.NUM_BYTES_LONG);
        return array;
    }

    @Override
    public DoubleArray newDoubleArray(long size, boolean clearOnResize) {
        final DoubleArray array = in.newDoubleArray(size, clearOnResize);
        onNewInstance(array, array.size() * RamUsageEstimator.NUM_BYTES_DOUBLE);
        return array;
    }

    @Override
    public FloatArray newFloatArray(long size, boolean clearOnResize) {
        final FloatArray array = in.newFloatArray(size, clearOnResize);
        onNewInstance(array, array.size() * RamUsageEstimator.NUM_BYTES_FLOAT);
        return array;
    }

    @Override
    public <T> ObjectArray<T> newObjectArray(long size) {
        final ObjectArray<T> array = in.newObjectArray(size);
        onNewInstance(array, array.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        return array;
    }

    @Override
    public ByteArray resize(ByteArray array, long size) {
        final long oldSize = array.size();
        final ByteArray resized = in.resize(array, size);
        onResize(resized, (resized.size() - oldSize) * RamUsageEstimator.NUM_BYTES_BYTE);
        return resized;
    }

    @Override
    public IntArray resize(IntArray array, long size) {
        final long oldSize = array.size();
        final IntArray resized = in.resize(array, size);
        onResize(resized, (resized.size() - oldSize) * RamUsageEstimator.NUM_BYTES_INT);
        return resized;
    }

    @Override
    public LongArray resize(LongArray array, long size) {
        final long oldSize = array.size();
        final LongArray resized = in.resize(array, size);
        onResize(resized, (resized.size() - oldSize) * RamUsageEstimator.NUM_BYTES_LONG);
        return resized;
    }

    @Override
    public DoubleArray resize(DoubleArray array, long size) {
        final long oldSize = array.size();
        final DoubleArray resized = in.resize(array, size);
        onResize(resized, (resized.size() - oldSize) * RamUsageEstimator.NUM_BYTES_DOUBLE);
        return resized;
    }

    @Override
    public FloatArray resize(FloatArray array, long size) {
        final long oldSize = array.size();
        final FloatArray resized = in.resize(array, size);
        onResize(resized, (resized.size() - oldSize) * RamUsageEstimator.NUM_BYTES_FLOAT);
        return resized;
    }

    /**
     * Return the total number of allocated array instances.
     */
    public long ramBytesAllocated() {
        return ramBytesAllocated(allocatedInstances.get(), allocatedBytes.get());
    }
}
