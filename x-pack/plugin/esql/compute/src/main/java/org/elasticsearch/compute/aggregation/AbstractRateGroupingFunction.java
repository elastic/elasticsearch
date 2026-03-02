/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

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
    abstract static class RawBuffer implements Releasable {
        final BigArrays bigArrays;
        LongArray timestamps;
        int valueCount;

        IntArray sliceStarts;
        IntArray sliceGroupIds;
        int sliceCount;
        int lastGroupId = -1;
        int minGroupId = Integer.MAX_VALUE;
        int maxGroupId = Integer.MIN_VALUE;

        RawBuffer(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            boolean success = false;
            try {
                this.timestamps = bigArrays.newLongArray(PageCacheRecycler.LONG_PAGE_SIZE, false);
                this.sliceStarts = bigArrays.newIntArray(512, false);
                this.sliceGroupIds = bigArrays.newIntArray(512, false);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        final void prepareSlicesOnly(int groupId, long firstTimestamp) {
            if (lastGroupId == groupId && valueCount > 0) {
                if (timestamps.get(valueCount - 1) > firstTimestamp) {
                    return; // continue with the current slice
                }
            }
            // start a new slice
            sliceStarts = bigArrays.grow(sliceStarts, sliceCount + 1L);
            sliceGroupIds = bigArrays.grow(sliceGroupIds, sliceCount + 1L);
            if (groupId < minGroupId) {
                minGroupId = groupId;
            }
            if (groupId > maxGroupId) {
                maxGroupId = groupId;
            }
            sliceStarts.set(sliceCount, valueCount);
            sliceGroupIds.set(sliceCount, groupId);
            lastGroupId = groupId;
            sliceCount++;
        }

        final FlushQueues prepareForFlush() {
            if (minGroupId > maxGroupId) {
                return new FlushQueues(this, minGroupId, maxGroupId, null, null);
            }
            final int numGroups = maxGroupId - minGroupId + 1;
            // count the number of slices in each group
            int[] runningOffsets = new int[numGroups];
            for (int i = 0; i < sliceCount; i++) {
                int groupIndex = sliceGroupIds.get(i) - minGroupId;
                runningOffsets[groupIndex]++;
            }
            int runningOffset = 0;
            for (int i = 0; i < numGroups; i++) {
                int count = runningOffsets[i];
                runningOffsets[i] = runningOffset;
                runningOffset += count;
            }
            IntArray sliceOffsets = bigArrays.newIntArray(sliceCount * 2L, false);
            for (int i = 0; i < sliceCount; i++) {
                int groupIndex = sliceGroupIds.get(i) - minGroupId;
                int startOffset = sliceStarts.get(i);
                final int endOffset;
                if ((i + 1) < sliceCount) {
                    endOffset = sliceStarts.get(i + 1);
                } else {
                    endOffset = valueCount;
                }
                int dstIndex = runningOffsets[groupIndex]++;
                sliceOffsets.set(dstIndex * 2L, startOffset);
                sliceOffsets.set(dstIndex * 2L + 1, endOffset);
            }
            valueCount = 0;
            sliceCount = 0;
            lastGroupId = -1;
            return new FlushQueues(this, minGroupId, maxGroupId, runningOffsets, sliceOffsets);
        }

        @Override
        public void close() {
            Releasables.close(timestamps, sliceStarts, sliceGroupIds);
        }
    }

    record FlushQueues(RawBuffer buffer, int minGroupId, int maxGroupId, int[] runningOffsets, IntArray sliceOffsets)
        implements
            Releasable {
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
                int start = sliceOffsets.get(i * 2L);
                int end = sliceOffsets.get(i * 2L + 1);
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

        @Override
        public void close() {
            Releasables.close(sliceOffsets);
        }
    }

    static final class Slice {
        int start;
        int end;
        long nextTimestamp;
        private long lastTimestamp = Long.MAX_VALUE;
        final LongArray timestamps;

        Slice(LongArray timestamps, int start, int end) {
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
}
