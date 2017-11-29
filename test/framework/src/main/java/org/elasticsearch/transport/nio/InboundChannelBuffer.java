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

import org.elasticsearch.common.lease.Releasable;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.function.Supplier;

public class InboundChannelBuffer {

    public static final int PAGE_SIZE = 1 << 14;
    private static final ByteBuffer[] EMPTY_BYTE_BUFFER_ARRAY = new ByteBuffer[0];

    private final int pageMask;
    private final int pageShift;

    private final ArrayDeque<Page> pages;
    private final Supplier<Page> pageSupplier;

    private long capacity = 0;
    private long internalIndex = 0;
    private int offset = 0;

    public InboundChannelBuffer() {
        this(() -> new Page(ByteBuffer.wrap(new byte[PAGE_SIZE]), () -> {}));
    }

    private InboundChannelBuffer(Supplier<Page> pageSupplier) {
        this.pageSupplier = pageSupplier;
        this.pages = new ArrayDeque<>();
        this.pageMask = PAGE_SIZE - 1;
        this.pageShift = Integer.numberOfTrailingZeros(PAGE_SIZE);
        this.capacity = PAGE_SIZE * pages.size();
        ensureCapacity(PAGE_SIZE);
    }

    public void ensureCapacity(long requiredCapacity) {
        if (capacity < requiredCapacity) {
            int numPages = numPages(requiredCapacity + offset);
            int pagesToAdd = numPages - pages.size();
            for (int i = 0; i < pagesToAdd; ++i) {
                pages.addLast(pageSupplier.get());
            }
            capacity += pagesToAdd * PAGE_SIZE;
        }
    }

    /**
     * This method will release bytes from the head of this buffer.
     *
     * @param bytesToRelease number of bytes to drop
     */
    public void release(long bytesToRelease) {
        if (bytesToRelease > capacity) {
            throw new IllegalArgumentException("Releasing more bytes [" + bytesToRelease + "] than buffer capacity [" + capacity + "].");
        }

        int pagesToRelease = pageIndex(offset + bytesToRelease);
        for (int i = 0; i < pagesToRelease; ++i) {
            Page page = pages.removeFirst();
            page.close();
        }
        capacity -= bytesToRelease;
        internalIndex = Math.max(internalIndex - bytesToRelease, 0);
        offset = indexInPage(bytesToRelease + offset);
    }

    /**
     * This method will return an array of {@link ByteBuffer} representing the bytes from the beginning of
     * this buffer up through the current index. The buffers will be duplicates of the internal buffers, so
     * any modifications to the markers {@link ByteBuffer#position()}, {@link ByteBuffer#limit()}, etc will
     * not modify the this class.
     *
     * @return the byte buffers
     */
    public ByteBuffer[] getPreIndexBuffers() {
        if (internalIndex == 0) {
            return EMPTY_BYTE_BUFFER_ARRAY;
        }
        long indexWithOffset = internalIndex + offset;
        int pageCount = pageIndex(indexWithOffset);
        int finalLimit = indexInPage(indexWithOffset);
        if (finalLimit != 0) {
            pageCount += 1;
        }

        ByteBuffer[] buffers = new ByteBuffer[pageCount];
        Iterator<Page> pageIterator = pages.iterator();
        ByteBuffer firstBuffer = pageIterator.next().buffer.duplicate();
        firstBuffer.position(firstBuffer.position() + offset);
        buffers[0] = firstBuffer;
        for (int i = 1; i < buffers.length; ++i) {
            buffers[i] = pageIterator.next().buffer.duplicate();
        }
        if (finalLimit != 0) {
            buffers[buffers.length - 1].limit(finalLimit);
        }

        return buffers;
    }

    /**
     * This method will return an array of {@link ByteBuffer} representing the bytes from the current index
     * of this buffer through the end. The buffers will be duplicates of the internal buffers, so any
     * modifications to the markers {@link ByteBuffer#position()}, {@link ByteBuffer#limit()}, etc will not
     * modify the this class.
     *
     * @return the byte buffers
     */
    public ByteBuffer[] getPostIndexBuffers() {
        if (internalIndex == capacity) {
            return new ByteBuffer[0];
        }
        long indexWithOffset = offset + internalIndex;

        int pageIndex = pageIndex(indexWithOffset);
        int indexInPage = indexInPage(indexWithOffset);

        ByteBuffer[] buffers = new ByteBuffer[pages.size() - pageIndex];
        Iterator<Page> pageIterator = pages.descendingIterator();
        for (int i = buffers.length - 1; i > 0; --i) {
            buffers[i] = pageIterator.next().buffer.duplicate();
        }
        ByteBuffer firstPostIndexBuffer = pageIterator.next().buffer.duplicate();
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
        return capacity - internalIndex;
    }

    private int numPages(long capacity) {
        final long numPages = (capacity + pageMask) >>> pageShift;
        if (numPages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pageSize=" + (pageMask + 1) + " is too small for such as capacity: " + capacity);
        }
        return (int) numPages;
    }

    private int pageIndex(long index) {
        return (int) (index >>> pageShift);
    }

    private int indexInPage(long index) {
        return (int) (index & pageMask);
    }

    private static class Page implements Releasable {

        private final ByteBuffer buffer;
        private final Releasable releasable;


        private Page(ByteBuffer buffer, Releasable releasable) {
            this.buffer = buffer;
            this.releasable = releasable;
        }

        @Override
        public void close() {
            releasable.close();
        }
    }
}
