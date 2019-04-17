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

package org.elasticsearch.nio;

import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.nio.utils.ExceptionsHelper;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This is a channel byte buffer composed internally of 16kb pages. When an entire message has been read
 * and consumed, the {@link #release(long)} method releases the bytes from the head of the buffer and closes
 * the pages internally. If more space is needed at the end of the buffer {@link #ensureCapacity(long)} can
 * be called and the buffer will expand using the supplier provided.
 */
public final class InboundChannelBuffer implements AutoCloseable {

    private static final int PAGE_SIZE = 1 << 14;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
    private static final ByteBuffer[] EMPTY_BYTE_BUFFER_ARRAY = new ByteBuffer[0];
    private static final Page[] EMPTY_BYTE_PAGE_ARRAY = new Page[0];


    private final ArrayDeque<Page> pages;
    private final Supplier<Page> pageSupplier;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private long capacity = 0;
    private long internalIndex = 0;
    // The offset is an int as it is the offset of where the bytes begin in the first buffer
    private int offset = 0;

    public InboundChannelBuffer(Supplier<Page> pageSupplier) {
        this.pageSupplier = pageSupplier;
        this.pages = new ArrayDeque<>();
        this.capacity = PAGE_SIZE * pages.size();
    }

    public static InboundChannelBuffer allocatingInstance() {
        return new InboundChannelBuffer(() -> new Page(ByteBuffer.allocate(PAGE_SIZE), () -> {}));
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            Page page;
            List<RuntimeException> closingExceptions = new ArrayList<>();
            while ((page = pages.pollFirst()) != null) {
                try {
                    page.close();
                } catch (RuntimeException e) {
                    closingExceptions.add(e);
                }
            }
            ExceptionsHelper.rethrowAndSuppress(closingExceptions);
        }
    }

    public void ensureCapacity(long requiredCapacity) {
        if (isClosed.get()) {
            throw new IllegalStateException("Cannot allocate new pages if the buffer is closed.");
        }
        if (capacity < requiredCapacity) {
            int numPages = numPages(requiredCapacity + offset);
            int pagesToAdd = numPages - pages.size();
            for (int i = 0; i < pagesToAdd; i++) {
                Page page = pageSupplier.get();
                pages.addLast(page);
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
            pages.removeFirst().close();
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
        Iterator<Page> pageIterator = pages.iterator();
        ByteBuffer firstBuffer = pageIterator.next().byteBuffer.duplicate();
        firstBuffer.position(firstBuffer.position() + offset);
        buffers[0] = firstBuffer;
        for (int i = 1; i < buffers.length; i++) {
            buffers[i] = pageIterator.next().byteBuffer.duplicate();
        }
        if (finalLimit != 0) {
            buffers[buffers.length - 1].limit(finalLimit);
        }

        return buffers;
    }

    /**
     * This method will return an array of {@link Page} representing the bytes from the beginning of
     * this buffer up through the index argument that was passed. The pages and buffers will be duplicates of
     * the internal components, so any modifications to the markers {@link ByteBuffer#position()},
     * {@link ByteBuffer#limit()}, etc will not modify the this class. Additionally, this will internally
     * retain the underlying pages, so the pages returned by this method must be closed.
     *
     * @param to the index to slice up to
     * @return the pages
     */
    public Page[] sliceAndRetainPagesTo(long to) {
        if (to > capacity) {
            throw new IndexOutOfBoundsException("can't slice a channel buffer with capacity [" + capacity +
                "], with slice parameters to [" + to + "]");
        } else if (to == 0) {
            return EMPTY_BYTE_PAGE_ARRAY;
        }
        long indexWithOffset = to + offset;
        int pageCount = pageIndex(indexWithOffset);
        int finalLimit = indexInPage(indexWithOffset);
        if (finalLimit != 0) {
            pageCount += 1;
        }

        Page[] pages = new Page[pageCount];
        Iterator<Page> pageIterator = this.pages.iterator();
        Page firstPage = pageIterator.next().duplicate();
        ByteBuffer firstBuffer = firstPage.byteBuffer;
        firstBuffer.position(firstBuffer.position() + offset);
        pages[0] = firstPage;
        for (int i = 1; i < pages.length; i++) {
            pages[i] = pageIterator.next().duplicate();
        }
        if (finalLimit != 0) {
            pages[pages.length - 1].byteBuffer.limit(finalLimit);
        }

        return pages;
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
        Iterator<Page> pageIterator = pages.descendingIterator();
        for (int i = buffers.length - 1; i > 0; --i) {
            buffers[i] = pageIterator.next().byteBuffer.duplicate();
        }
        ByteBuffer firstPostIndexBuffer = pageIterator.next().byteBuffer.duplicate();
        firstPostIndexBuffer.position(firstPostIndexBuffer.position() + indexInPage);
        buffers[0] = firstPostIndexBuffer;

        return buffers;
    }

    /**
     * Aligns the data in this buffer along the underlying pages. If the beginning of this buffer is in the
     * middle of a page, it will be copied to the beginning of a new page. The amount of copied into
     * alignment with the underlying pages is configured by the length parameter. As this operation might add
     * or remove pages, the it can modify the buffers capacity.
     *
     * @param length of the data to align
     */
    public void align(long length) {
        if (offset == 0 || pages.size() == 0) {
            return;
        }

        if (pageIndex(length + offset) == 0) {
            Page oldPage = pages.removeFirst();
            ByteBuffer oldByteBuffer = oldPage.getByteBuffer().duplicate();
            oldByteBuffer.position(offset);
            oldByteBuffer.limit(oldByteBuffer.position() + Math.toIntExact(length));
            Page newPage = pageSupplier.get();
            newPage.getByteBuffer().put(oldByteBuffer);
            newPage.getByteBuffer().clear();
            pages.addFirst(newPage);
            oldPage.close();
        } else {
            long indexWithOffset = length + offset;
            int newPageCount = pageIndex(length);
            int finalLimit = indexInPage(indexWithOffset);
            if (finalLimit != 0) {
                newPageCount += 1;
            }
            Page[] newPages = new Page[newPageCount];

            Page newPage = pageSupplier.get();
            ByteBuffer newByteBuffer = newPage.getByteBuffer();
            Page oldPage = pages.removeFirst();
            ByteBuffer oldByteBuffer = oldPage.getByteBuffer().duplicate();
            oldByteBuffer.position(offset);
            long bytesToCopy = internalIndex;
            int newBufferIndex = 0;
            while (bytesToCopy > 0) {
                if (newByteBuffer.remaining() == 0) {
                    newPages[newBufferIndex] = newPage;
                    newPage = pageSupplier.get();
                    newByteBuffer = newPage.getByteBuffer();
                    ++newBufferIndex;
                }
                if (oldByteBuffer.remaining() == 0) {
                    oldPage.close();
                    oldPage = pages.removeFirst();
                    oldByteBuffer = oldPage.getByteBuffer().duplicate();
                }

                int limitDelta = Math.min(Math.min((int) bytesToCopy, oldByteBuffer.remaining()), newByteBuffer.remaining());
                oldByteBuffer.limit(oldByteBuffer.position() + limitDelta);
                newByteBuffer.put(oldByteBuffer);
                bytesToCopy -= limitDelta;
                oldByteBuffer.limit(oldByteBuffer.capacity());
            }

            newPages[newBufferIndex] = newPage;

            for (int i = newPageCount - 1; i >= 0; --i) {
                Page page = newPages[i];
                page.getByteBuffer().clear();
                pages.addFirst(newPage);
            }
        }

        offset = 0;
        capacity = pages.size() * PAGE_SIZE;
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

    public static class Page implements AutoCloseable {

        private final ByteBuffer byteBuffer;
        // This is reference counted as some implementations want to retain the byte pages by calling
        // sliceAndRetainPagesTo. With reference counting we can increment the reference count, return the
        // pages, and safely close them when this channel buffer is done with them. The reference count
        // would be 1 at that point, meaning that the pages will remain until the implementation closes
        // theirs.
        private final RefCountedCloseable refCountedCloseable;

        public Page(ByteBuffer byteBuffer, Runnable closeable) {
            this(byteBuffer, new RefCountedCloseable(closeable));
        }

        private Page(ByteBuffer byteBuffer, RefCountedCloseable refCountedCloseable) {
            this.byteBuffer = byteBuffer;
            this.refCountedCloseable = refCountedCloseable;
        }

        private Page duplicate() {
            refCountedCloseable.incRef();
            return new Page(byteBuffer.duplicate(), refCountedCloseable);
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        @Override
        public void close() {
            refCountedCloseable.decRef();
        }

        private static class RefCountedCloseable extends AbstractRefCounted {

            private final Runnable closeable;

            private RefCountedCloseable(Runnable closeable) {
                super("byte array page");
                this.closeable = closeable;
            }

            @Override
            protected void closeInternal() {
                closeable.run();
            }
        }
    }
}
