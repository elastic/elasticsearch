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

package org.elasticsearch.common.lab;

import org.elasticsearch.common.Preconditions;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LongsLAB {

    private AtomicReference<Chunk> curChunk = new AtomicReference<Chunk>();

    final int chunkSize;
    final int maxAlloc;

    public LongsLAB(int chunkSize, int maxAlloc) {
        this.chunkSize = chunkSize;
        this.maxAlloc = maxAlloc;
        // if we don't exclude allocations >CHUNK_SIZE, we'd infiniteloop on one!
        Preconditions.checkArgument(maxAlloc <= chunkSize);
    }

    /**
     * Allocate a slice of the given length.
     * <p/>
     * If the size is larger than the maximum size specified for this
     * allocator, returns null.
     */
    public Allocation allocateLongs(int size) {
        Preconditions.checkArgument(size >= 0, "negative size");

        // Callers should satisfy large allocations directly from JVM since they
        // don't cause fragmentation as badly.
        if (size > maxAlloc) {
            return null;
        }

        while (true) {
            Chunk c = getOrMakeChunk();

            // Try to allocate from this chunk
            int allocOffset = c.alloc(size);
            if (allocOffset != -1) {
                // We succeeded - this is the common case - small alloc
                // from a big buffer
                return new Allocation(c.data, allocOffset);
            }

            // not enough space!
            // try to retire this chunk
            tryRetireChunk(c);
        }
    }

    /**
     * Try to retire the current chunk if it is still
     * <code>c</code>. Postcondition is that curChunk.get()
     * != c
     */
    private void tryRetireChunk(Chunk c) {
        @SuppressWarnings("unused")
        boolean weRetiredIt = curChunk.compareAndSet(c, null);
        // If the CAS succeeds, that means that we won the race
        // to retire the chunk. We could use this opportunity to
        // update metrics on external fragmentation.
        //
        // If the CAS fails, that means that someone else already
        // retired the chunk for us.
    }

    /**
     * Get the current chunk, or, if there is no current chunk,
     * allocate a new one from the JVM.
     */
    private Chunk getOrMakeChunk() {
        while (true) {
            // Try to get the chunk
            Chunk c = curChunk.get();
            if (c != null) {
                return c;
            }

            // No current chunk, so we want to allocate one. We race
            // against other allocators to CAS in an uninitialized chunk
            // (which is cheap to allocate)
            c = new Chunk(chunkSize);
            if (curChunk.compareAndSet(null, c)) {
                // we won race - now we need to actually do the expensive
                // allocation step
                c.init();
                return c;
            }
            // someone else won race - that's fine, we'll try to grab theirs
            // in the next iteration of the loop.
        }
    }

    /**
     * A chunk of memory out of which allocations are sliced.
     */
    private static class Chunk {
        /**
         * Actual underlying data
         */
        private long[] data;

        private static final int UNINITIALIZED = -1;
        private static final int OOM = -2;
        /**
         * Offset for the next allocation, or the sentinel value -1
         * which implies that the chunk is still uninitialized.
         */
        private AtomicInteger nextFreeOffset = new AtomicInteger(UNINITIALIZED);

        /**
         * Total number of allocations satisfied from this buffer
         */
        private AtomicInteger allocCount = new AtomicInteger();

        /**
         * Size of chunk in longs
         */
        private final int size;

        /**
         * Create an uninitialized chunk. Note that memory is not allocated yet, so
         * this is cheap.
         *
         * @param size in longs
         */
        private Chunk(int size) {
            this.size = size;
        }

        /**
         * Actually claim the memory for this chunk. This should only be called from
         * the thread that constructed the chunk. It is thread-safe against other
         * threads calling alloc(), who will block until the allocation is complete.
         */
        public void init() {
            assert nextFreeOffset.get() == UNINITIALIZED;
            try {
                data = new long[size];
            } catch (OutOfMemoryError e) {
                boolean failInit = nextFreeOffset.compareAndSet(UNINITIALIZED, OOM);
                assert failInit; // should be true.
                throw e;
            }
            // Mark that it's ready for use
            boolean initted = nextFreeOffset.compareAndSet(
                    UNINITIALIZED, 0);
            // We should always succeed the above CAS since only one thread
            // calls init()!
            Preconditions.checkState(initted,
                    "Multiple threads tried to init same chunk");
        }

        /**
         * Try to allocate <code>size</code> longs from the chunk.
         *
         * @return the offset of the successful allocation, or -1 to indicate not-enough-space
         */
        public int alloc(int size) {
            while (true) {
                int oldOffset = nextFreeOffset.get();
                if (oldOffset == UNINITIALIZED) {
                    // The chunk doesn't have its data allocated yet.
                    // Since we found this in curChunk, we know that whoever
                    // CAS-ed it there is allocating it right now. So spin-loop
                    // shouldn't spin long!
                    Thread.yield();
                    continue;
                }
                if (oldOffset == OOM) {
                    // doh we ran out of ram. return -1 to chuck this away.
                    return -1;
                }

                if (oldOffset + size > data.length) {
                    return -1; // alloc doesn't fit
                }

                // Try to atomically claim this chunk
                if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size)) {
                    // we got the alloc
                    allocCount.incrementAndGet();
                    return oldOffset;
                }
                // we raced and lost alloc, try again
            }
        }

        @Override
        public String toString() {
            return "Chunk@" + System.identityHashCode(this) +
                    " allocs=" + allocCount.get() + "waste=" +
                    (data.length - nextFreeOffset.get());
        }
    }

    /**
     * The result of a single allocation. Contains the chunk that the
     * allocation points into, and the offset in this array where the
     * slice begins.
     */
    public static class Allocation {
        private final long[] data;
        private final int offset;

        private Allocation(long[] data, int off) {
            this.data = data;
            this.offset = off;
        }

        @Override
        public String toString() {
            return "Allocation(data=" + data +
                    " with capacity=" + data.length +
                    ", off=" + offset + ")";
        }

        public long[] getData() {
            return data;
        }

        public int getOffset() {
            return offset;
        }
    }

}