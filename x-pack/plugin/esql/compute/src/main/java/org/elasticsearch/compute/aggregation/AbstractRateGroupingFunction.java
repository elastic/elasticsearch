/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.util.Arrays;

import static org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.BufferedArray.indexInPage;
import static org.elasticsearch.compute.aggregation.AbstractRateGroupingFunction.BufferedArray.pageIndex;

class AbstractRateGroupingFunction {
    /**
     * Buffers data points in two arrays: one for timestamps and one for values, partitioned into multiple slices.
     * Each slice is sorted in descending order of timestamp. A new slice is created when a data point has a
     * timestamp greater than the last point of the current slice or it belongs to a different group (=timeseries).
     * Since each page is sorted by descending timestamp,
     * we only need to compare the first point of the new page with the last point of the current slice to decide
     * if a new slice is needed. During merging, a priority queue is used to iterate through the slices, selecting
     * the slice with the greatest timestamp.
     */
    static final int INITIAL_SLICE_CAPACITY = 512;

    abstract static class RawBuffer implements Releasable {
        private final CircuitBreaker breaker;
        private long acquiredBytes;
        LongBuffer timestamps;

        int[] sliceStarts;
        int[] sliceGroupIds;
        int sliceCount;
        int lastGroupId = -1;
        int minGroupId = Integer.MAX_VALUE;
        int maxGroupId = Integer.MIN_VALUE;

        RawBuffer(CircuitBreaker breaker) {
            this.breaker = breaker;
            this.acquiredBytes = (long) INITIAL_SLICE_CAPACITY * Integer.BYTES * 2;
            breaker.addEstimateBytesAndMaybeBreak(acquiredBytes, "rate-slices");
            this.sliceStarts = new int[INITIAL_SLICE_CAPACITY];
            this.sliceGroupIds = new int[INITIAL_SLICE_CAPACITY];
            this.timestamps = new LongBuffer(breaker, PAGE_SIZE);
        }

        final void prepareSlicesOnly(int groupId, long firstTimestamp) {
            if (lastGroupId == groupId && timestamps.size() > 0) {
                if (timestamps.get(timestamps.size() - 1) > firstTimestamp) {
                    return; // continue with the current slice
                }
            }
            if (sliceCount >= sliceStarts.length) {
                int newLen = sliceStarts.length * 2;
                long delta = (long) (newLen - sliceStarts.length) * Integer.BYTES * 2;
                breaker.addEstimateBytesAndMaybeBreak(delta, "rate-slices");
                acquiredBytes += delta;
                sliceStarts = Arrays.copyOf(sliceStarts, newLen);
                sliceGroupIds = Arrays.copyOf(sliceGroupIds, newLen);
            }
            if (groupId < minGroupId) {
                minGroupId = groupId;
            }
            if (groupId > maxGroupId) {
                maxGroupId = groupId;
            }
            sliceStarts[sliceCount] = timestamps.size();
            sliceGroupIds[sliceCount] = groupId;
            lastGroupId = groupId;
            sliceCount++;
        }

        final FlushQueues prepareForFlush() {
            if (minGroupId > maxGroupId) {
                return new FlushQueues(this, minGroupId, maxGroupId, null, null);
            }
            final int numGroups = maxGroupId - minGroupId + 1;
            int[] runningOffsets = new int[numGroups];
            for (int i = 0; i < sliceCount; i++) {
                runningOffsets[sliceGroupIds[i] - minGroupId]++;
            }
            int runningOffset = 0;
            for (int i = 0; i < numGroups; i++) {
                int count = runningOffsets[i];
                runningOffsets[i] = runningOffset;
                runningOffset += count;
            }
            int[] sliceOffsets = new int[sliceCount * 2];
            for (int i = 0; i < sliceCount; i++) {
                int groupIndex = sliceGroupIds[i] - minGroupId;
                int startOffset = sliceStarts[i];
                int endOffset = (i + 1) < sliceCount ? sliceStarts[i + 1] : timestamps.size();
                int dstIndex = runningOffsets[groupIndex]++;
                sliceOffsets[dstIndex * 2] = startOffset;
                sliceOffsets[dstIndex * 2 + 1] = endOffset;
            }
            sliceCount = 0;
            lastGroupId = -1;
            var queues = new FlushQueues(this, minGroupId, maxGroupId, runningOffsets, sliceOffsets);
            minGroupId = Integer.MAX_VALUE;
            maxGroupId = Integer.MIN_VALUE;
            return queues;
        }

        abstract void clearBuffers();

        @Override
        public void close() {
            Releasables.close(timestamps);
            breaker.addWithoutBreaking(-acquiredBytes);
            acquiredBytes = 0;
        }
    }

    record FlushQueues(RawBuffer buffer, int minGroupId, int maxGroupId, int[] runningOffsets, int[] sliceOffsets) {
        FlushQueue getFlushQueue(int groupId) {
            if (groupId < minGroupId || groupId > maxGroupId) {
                return null;
            }
            int groupIndex = groupId - minGroupId;
            int endIndex = runningOffsets[groupIndex];
            int startIndex = groupIndex == 0 ? 0 : runningOffsets[groupIndex - 1];
            int numSlices = endIndex - startIndex;
            if (numSlices == 0) {
                return null;
            }
            FlushQueue queue = new FlushQueue(numSlices);
            for (int i = startIndex; i < endIndex; i++) {
                int start = sliceOffsets[i * 2];
                int end = sliceOffsets[i * 2 + 1];
                if (start < end) {
                    queue.valueCount += (end - start);
                    queue.add(new Slice(buffer.timestamps, start, end));
                }
            }
            if (queue.valueCount == 0) {
                return null;
            }
            return queue;
        }
    }

    static final class Slice {
        int start;
        int end;
        long nextTimestamp;
        private long lastTimestamp = Long.MAX_VALUE;
        final LongBuffer timestamps;

        Slice(LongBuffer timestamps, int start, int end) {
            this.timestamps = timestamps;
            this.start = start;
            this.end = end;
            this.nextTimestamp = timestamps.get(start);
        }

        boolean exhausted() {
            return start >= end;
        }

        int next() {
            int currentIndex = start;
            start++;
            if (start < end) {
                nextTimestamp = timestamps.get(start); // next timestamp
            }
            return currentIndex;
        }

        long lastTimestamp() {
            if (lastTimestamp == Long.MAX_VALUE) {
                lastTimestamp = timestamps.get(end - 1);
            }
            return lastTimestamp;
        }
    }

    static final class FlushQueue extends PriorityQueue<Slice> {
        int valueCount;

        FlushQueue(int maxSize) {
            super(maxSize);
        }

        /**
         * Returns the timestamp of the slice that would be next in line after the best slice.
         */
        long secondNextTimestamp() {
            final Object[] heap = getHeapArray();
            final int size = size();
            if (size == 2) {
                return ((Slice) heap[2]).nextTimestamp;
            } else if (size >= 3) {
                return Math.max(((Slice) heap[2]).nextTimestamp, ((Slice) heap[3]).nextTimestamp);
            } else {
                return Long.MIN_VALUE;
            }
        }

        @Override
        protected boolean lessThan(Slice a, Slice b) {
            return a.nextTimestamp > b.nextTimestamp; // want the latest timestamp first
        }
    }

    static final int PAGE_SHIFT = 12;
    static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    static final int PAGE_MASK = PAGE_SIZE - 1;

    abstract static class BufferedArray implements Releasable {
        protected final CircuitBreaker breaker;
        private long acquiredBytes;
        protected long capacity;
        protected int numPages;
        int size;

        BufferedArray(CircuitBreaker breaker, long initialCapacity, int bytesPerElement) {
            this.breaker = breaker;
            this.numPages = Math.max(1, Math.toIntExact((initialCapacity + PAGE_SIZE - 1) >>> PAGE_SHIFT));
            long bytes = (long) numPages * PAGE_SIZE * bytesPerElement;
            breaker.addEstimateBytesAndMaybeBreak(bytes, "buffered-array");
            acquiredBytes = bytes;
            capacity = (long) numPages << PAGE_SHIFT;
        }

        final void grow(long minCapacity, int bytesPerElement) {
            if (minCapacity <= capacity) {
                return;
            }
            long newSize = BigArrays.overSize(minCapacity, PageCacheRecycler.BYTE_PAGE_SIZE, bytesPerElement);
            int newNumPages = Math.toIntExact((newSize + PAGE_SIZE - 1) >>> PAGE_SHIFT);
            long bytes = (long) (newNumPages - numPages) * PAGE_SIZE * bytesPerElement;
            breaker.addEstimateBytesAndMaybeBreak(bytes, "buffered-array");
            acquiredBytes += bytes;
            numPages = newNumPages;
            capacity = (long) newNumPages << PAGE_SHIFT;
        }

        final int size() {
            return size;
        }

        final void clear() {
            size = 0;
        }

        @Override
        public void close() {
            if (acquiredBytes > 0) {
                breaker.addWithoutBreaking(-acquiredBytes);
                acquiredBytes = 0;
            }
        }

        static int pageIndex(long index) {
            return (int) (index >>> PAGE_SHIFT);
        }

        static int indexInPage(long index) {
            return (int) (index & PAGE_MASK);
        }
    }

    /**
     * Append-only paged long array backed by {@code long[][]}.
     * Avoids the VarHandle byte[]-backed LongArray and leverages {@link System#arraycopy} when possible.
     */
    static final class LongBuffer extends BufferedArray {
        long[][] pages;

        LongBuffer(CircuitBreaker breaker, long initialCapacity) {
            super(breaker, initialCapacity, Long.BYTES);
            pages = new long[numPages][];
            for (int i = 0; i < numPages; i++) {
                pages[i] = new long[PAGE_SIZE];
            }
        }

        long get(long index) {
            assert index < size : "Index [" + index + "] out of bounds, size: " + size;
            return pages[pageIndex(index)][indexInPage(index)];
        }

        void append(long value) {
            pages[pageIndex(size)][indexInPage(size)] = value;
            size++;
        }

        void appendRange(LongVector src, int from, int count) {
            int indexInPageStart = indexInPage(size);
            if (indexInPageStart + count <= PAGE_SIZE) {
                src.copyTo(from, pages[pageIndex(size)], indexInPageStart, count);
            } else {
                for (int i = 0; i < count; i++) {
                    long d = size + i;
                    pages[pageIndex(d)][indexInPage(d)] = src.getLong(from + i);
                }
            }
            size += count;
        }

        void ensureCapacity(long minCapacity) {
            int oldNumPages = numPages;
            grow(minCapacity, Long.BYTES);
            if (numPages > oldNumPages) {
                pages = Arrays.copyOf(pages, numPages);
                for (int i = oldNumPages; i < numPages; i++) {
                    pages[i] = new long[PAGE_SIZE];
                }
            }
        }
    }

    /**
     * Append-only paged double array backed by {@code double[][]}.
     * Avoids the VarHandle byte[]-backed DoubleArray and leverages {@link System#arraycopy} when possible.
     */
    static final class DoubleBuffer extends BufferedArray {
        double[][] pages;

        DoubleBuffer(CircuitBreaker breaker, long initialCapacity) {
            super(breaker, initialCapacity, Double.BYTES);
            pages = new double[numPages][];
            for (int i = 0; i < numPages; i++) {
                pages[i] = new double[PAGE_SIZE];
            }
        }

        double get(long index) {
            assert index < size : "Index [" + index + "] out of bounds, size: " + size;
            return pages[pageIndex(index)][indexInPage(index)];
        }

        void append(double value) {
            pages[pageIndex(size)][indexInPage(size)] = value;
            size++;
        }

        void appendRange(DoubleVector src, int from, int count) {
            int indexInPageStart = indexInPage(size);
            if (indexInPageStart + count <= PAGE_SIZE) {
                src.copyTo(from, pages[pageIndex(size)], indexInPageStart, count);
            } else {
                for (int i = 0; i < count; i++) {
                    long d = size + i;
                    pages[pageIndex(d)][indexInPage(d)] = src.getDouble(from + i);
                }
            }
            size += count;
        }

        void ensureCapacity(long minCapacity) {
            int oldNumPages = numPages;
            grow(minCapacity, Double.BYTES);
            if (numPages > oldNumPages) {
                pages = Arrays.copyOf(pages, numPages);
                for (int i = oldNumPages; i < numPages; i++) {
                    pages[i] = new double[PAGE_SIZE];
                }
            }
        }
    }

    /**
     * Append-only paged int array backed by {@code int[][]}.
     * Avoids the VarHandle byte[]-backed IntArray and leverages {@link System#arraycopy} when possible.
     */
    static final class IntBuffer extends BufferedArray {
        int[][] pages;

        IntBuffer(CircuitBreaker breaker, long initialCapacity) {
            super(breaker, initialCapacity, Integer.BYTES);
            pages = new int[numPages][];
            for (int i = 0; i < numPages; i++) {
                pages[i] = new int[PAGE_SIZE];
            }
        }

        int get(long index) {
            assert index < size : "Index [" + index + "] out of bounds, size: " + size;
            return pages[pageIndex(index)][indexInPage(index)];
        }

        void append(int value) {
            pages[pageIndex(size)][indexInPage(size)] = value;
            size++;
        }

        void appendRange(IntVector src, int from, int count) {
            int indexInPageStart = indexInPage(size);
            if (indexInPageStart + count <= PAGE_SIZE) {
                src.copyTo(from, pages[pageIndex(size)], indexInPageStart, count);
            } else {
                for (int i = 0; i < count; i++) {
                    long d = size + i;
                    pages[pageIndex(d)][indexInPage(d)] = src.getInt(from + i);
                }
            }
            size += count;
        }

        void ensureCapacity(long minCapacity) {
            int oldNumPages = numPages;
            grow(minCapacity, Integer.BYTES);
            if (numPages > oldNumPages) {
                pages = Arrays.copyOf(pages, numPages);
                for (int i = oldNumPages; i < numPages; i++) {
                    pages[i] = new int[PAGE_SIZE];
                }
            }
        }
    }

    /**
     * Append-only paged buffer for {@link ExponentialHistogram} values backed by {@link ExponentialHistogramBlock} pages.
     * Pages are built lazily via a block builder and finalized when full or when a read is requested.
     */
    static final class ExponentialHistogramBuffer implements Releasable {

        private final BlockFactory factory;
        private ExponentialHistogramBlock[] pages;
        private ExponentialHistogramBlock.Builder activeBuilder;
        private int size = 0;

        ExponentialHistogramBuffer(BlockFactory factory, long initialCapacity) {
            this.factory = factory;
            int initialNumPages = Math.max(1, Math.toIntExact((initialCapacity + PAGE_SIZE - 1) >>> PAGE_SHIFT));
            pages = new ExponentialHistogramBlock[initialNumPages];
            size = 0;
        }

        int size() {
            return size;
        }

        ExponentialHistogram get(long index, ExponentialHistogramScratch scratch) {
            assert index < size : "Index [" + index + "] out of bounds, size: " + size;
            buildCurrentPage();
            return pages[pageIndex(index)].getExponentialHistogram(indexInPage(index), scratch);
        }

        void append(ExponentialHistogram value) {
            initBuilderForNextPosition();
            activeBuilder.append(value);
            size++;
            if (size % PAGE_SIZE == 0) {
                buildCurrentPage();
            }
        }

        void appendRange(ExponentialHistogramBlock src, int from, int count) {
            assert assertSingleValued(src, from, count);
            int freeSlotsOnCurrentPage = PAGE_SIZE - indexInPage(size);
            int pos = from;
            int remainingCount = count;
            while (remainingCount >= freeSlotsOnCurrentPage) {
                initBuilderForNextPosition();
                activeBuilder.copyFrom(src, pos, pos + freeSlotsOnCurrentPage);
                size += freeSlotsOnCurrentPage;
                pos += freeSlotsOnCurrentPage;
                remainingCount -= freeSlotsOnCurrentPage;
                freeSlotsOnCurrentPage = PAGE_SIZE;
                buildCurrentPage();
            }

            if (remainingCount > 0) {
                initBuilderForNextPosition();
                activeBuilder.copyFrom(src, pos, pos + remainingCount);
                size += remainingCount;
            }
        }

        private boolean assertSingleValued(ExponentialHistogramBlock src, int from, int count) {
            for (int i = from; i < from + count; i++) {
                assert src.isNull(i) == false : "Tried to append null value to ExponentialHistogramBuffer";
                assert src.getValueCount(i) == 1 : "Tried to append multi-value to ExponentialHistogramBuffer";
            }
            return true;
        }

        private void initBuilderForNextPosition() {
            if (activeBuilder == null) {
                assert size % PAGE_SIZE == 0 : "Cannot append after reading";
                activeBuilder = factory.newExponentialHistogramBlockBuilder(PAGE_SIZE);
            }
        }

        private void buildCurrentPage() {
            if (activeBuilder != null) {
                int pageIdx = pageIndex(size - 1);
                if (pageIdx >= pages.length) {
                    pages = Arrays.copyOf(pages, pageIdx * 2);
                }
                assert pages[pageIdx] == null;
                pages[pageIdx] = activeBuilder.build();
                activeBuilder = null;
            }
        }

        void ensureCapacity(long minCapacity) {
            // Pages are allocated on demand automatically via the block builder
        }

        void clear() {
            size = 0;
            Releasables.close(activeBuilder);
            Releasables.close(pages);
            activeBuilder = null;
            Arrays.fill(pages, null);
        }

        @Override
        public void close() {
            Releasables.close(activeBuilder);
            Releasables.close(pages);
        }
    }

}
