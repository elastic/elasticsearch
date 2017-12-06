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

package org.elasticsearch.transport.nio;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * This is a channel byte buffer composed internally of 16kb pages. When an entire message has been read
 * and consumed, the {@link #release(long)} method releases the bytes from the head of the buffer and closes
 * the pages internally. If more space is needed at the end of the buffer {@link #ensureCapacity(long)} can
 * be called and the buffer will expand using the supplier provided.
 */
public final class InboundChannelBuffer {

    private static final int PAGE_SIZE = 1 << 14;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
    private static final ByteBuffer[] EMPTY_BYTE_BUFFER_ARRAY = new ByteBuffer[0];


    private final ArrayDeque<ByteBuffer> pages;
    private final Supplier<ByteBuffer> pageSupplier;

    private long capacity = 0;
    private long internalIndex = 0;
    // The offset is an int as it is the offset of where the bytes begin in the first buffer
    private int offset = 0;

    public InboundChannelBuffer() {
        this(() -> ByteBuffer.wrap(new byte[PAGE_SIZE]));
    }

    private InboundChannelBuffer(Supplier<ByteBuffer> pageSupplier) {
        this.pageSupplier = pageSupplier;
        this.pages = new ArrayDeque<>();
        this.capacity = PAGE_SIZE * pages.size();
        ensureCapacity(PAGE_SIZE);
    }

    public void ensureCapacity(long requiredCapacity) {
        if (capacity < requiredCapacity) {
            int numPages = numPages(requiredCapacity + offset);
            int pagesToAdd = numPages - pages.size();
            for (int i = 0; i < pagesToAdd; i++) {
                pages.addLast(pageSupplier.get());
            }
            capacity += pagesToAdd * PAGE_SIZE;
        }
    }

    /**
     * This method will release bytes from the head of this buffer. If you release bytes past the current
     * index the index is truncated to zero.
     *
     * @param bytesToRelease number of bytes to drop
     */
    public void release(long bytesToRelease) {
        if (bytesToRelease > capacity) {
            throw new IllegalArgumentException("Releasing more bytes [" + bytesToRelease + "] than buffer capacity [" + capacity + "].");
        }

        int pagesToRelease = pageIndex(offset + bytesToRelease);
        for (int i = 0; i < pagesToRelease; i++) {
            pages.removeFirst();
        }
        capacity -= bytesToRelease;
        internalIndex = Math.max(internalIndex - bytesToRelease, 0);
        offset = indexInPage(bytesToRelease + offset);
    }

    /**
     * This method will return an array of {@link ByteBuffer} representing the bytes from the beginning of
     * this buffer up through the index argument that was passed. The buffers will be duplicates of the
     * internal buffers, so any modifications to the markers {@link ByteBuffer#position()},
     * {@link ByteBuffer#limit()}, etc will not modify the this class.
     *
     * @param to the index to slice up to
     * @return the byte buffers
     */
    public ByteBuffer[] sliceBuffersTo(long to) {
        if (to > capacity) {
            throw new IndexOutOfBoundsException("can't slice a channel buffer with capacity [" + capacity +
                "], with slice parameters to [" + to + "]");
        } else if (to == 0) {
            return EMPTY_BYTE_BUFFER_ARRAY;
        }
        long indexWithOffset = to + offset;
        int pageCount = pageIndex(indexWithOffset);
        int finalLimit = indexInPage(indexWithOffset);
        if (finalLimit != 0) {
            pageCount += 1;
        }

        ByteBuffer[] buffers = new ByteBuffer[pageCount];
        Iterator<ByteBuffer> pageIterator = pages.iterator();
        ByteBuffer firstBuffer = pageIterator.next().duplicate();
        firstBuffer.position(firstBuffer.position() + offset);
        buffers[0] = firstBuffer;
        for (int i = 1; i < buffers.length; i++) {
            buffers[i] = pageIterator.next().duplicate();
        }
        if (finalLimit != 0) {
            buffers[buffers.length - 1].limit(finalLimit);
        }

        return buffers;
    }

    /**
     * This method will return an array of {@link ByteBuffer} representing the bytes from the index passed
     * through the end of this buffer. The buffers will be duplicates of the internal buffers, so any
     * modifications to the markers {@link ByteBuffer#position()}, {@link ByteBuffer#limit()}, etc will not
     * modify the this class.
     *
     * @param from the index to slice from
     * @return the byte buffers
     */
    public ByteBuffer[] sliceBuffersFrom(long from) {
        if (from > capacity) {
            throw new IndexOutOfBoundsException("can't slice a channel buffer with capacity [" + capacity +
                "], with slice parameters from [" + from + "]");
        } else if (from == capacity) {
            return EMPTY_BYTE_BUFFER_ARRAY;
        }
        long indexWithOffset = from + offset;

        int pageIndex = pageIndex(indexWithOffset);
        int indexInPage = indexInPage(indexWithOffset);

        ByteBuffer[] buffers = new ByteBuffer[pages.size() - pageIndex];
        Iterator<ByteBuffer> pageIterator = pages.descendingIterator();
        for (int i = buffers.length - 1; i > 0; --i) {
            buffers[i] = pageIterator.next().duplicate();
        }
        ByteBuffer firstPostIndexBuffer = pageIterator.next().duplicate();
        firstPostIndexBuffer.position(firstPostIndexBuffer.position() + indexInPage);
        buffers[0] = firstPostIndexBuffer;

        return buffers;
    }

    public void incrementIndex(long delta) {
        if (delta < 0) {
            throw new IllegalArgumentException("Cannot increment an index with a negative delta [" + delta + "]");
        }

        long newIndex = delta + internalIndex;
        if (newIndex > capacity) {
            throw new IllegalArgumentException("Cannot increment an index [" + internalIndex + "] with a delta [" + delta +
                "] that will result in a new index [" + newIndex + "] that is greater than the capacity [" + capacity + "].");
        }
        internalIndex = newIndex;
    }

    public long getIndex() {
        return internalIndex;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getRemaining() {
        long remaining = capacity - internalIndex;
        assert remaining >= 0 : "The remaining [" + remaining + "] number of bytes should not be less than zero.";
        return remaining;
    }

    private int numPages(long capacity) {
        final long numPages = (capacity + PAGE_MASK) >>> PAGE_SHIFT;
        if (numPages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pageSize=" + (PAGE_MASK + 1) + " is too small for such as capacity: " + capacity);
        }
        return (int) numPages;
    }

    private int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }
}
