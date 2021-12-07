/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import org.elasticsearch.nio.utils.ExceptionsHelper;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

/**
 * This is a channel byte buffer composed internally of 16kb pages. When an entire message has been read
 * and consumed, the {@link #release(long)} method releases the bytes from the head of the buffer and closes
 * the pages internally. If more space is needed at the end of the buffer {@link #ensureCapacity(long)} can
 * be called and the buffer will expand using the supplier provided.
 */
public final class InboundChannelBuffer implements AutoCloseable {

    public static final int PAGE_SIZE = 1 << 14;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
    private static final ByteBuffer[] EMPTY_BYTE_BUFFER_ARRAY = new ByteBuffer[0];
    private static final Page[] EMPTY_BYTE_PAGE_ARRAY = new Page[0];

    private final IntFunction<Page> pageAllocator;
    private final ArrayDeque<Page> pages = new ArrayDeque<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private long capacity = 0;
    private long internalIndex = 0;
    // The offset is an int as it is the offset of where the bytes begin in the first buffer
    private int offset = 0;

    public InboundChannelBuffer(IntFunction<Page> pageAllocator) {
        this.pageAllocator = pageAllocator;
    }

    public static InboundChannelBuffer allocatingInstance() {
        return new InboundChannelBuffer((n) -> new Page(ByteBuffer.allocate(n), () -> {}));
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
                Page page = pageAllocator.apply(PAGE_SIZE);
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
            throw new IndexOutOfBoundsException(
                "can't slice a channel buffer with capacity [" + capacity + "], with slice parameters to [" + to + "]"
            );
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
        ByteBuffer firstBuffer = pageIterator.next().byteBuffer().duplicate();
        firstBuffer.position(firstBuffer.position() + offset);
        buffers[0] = firstBuffer;
        for (int i = 1; i < buffers.length; i++) {
            buffers[i] = pageIterator.next().byteBuffer().duplicate();
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
            throw new IndexOutOfBoundsException(
                "can't slice a channel buffer with capacity [" + capacity + "], with slice parameters to [" + to + "]"
            );
        } else if (to == 0) {
            return EMPTY_BYTE_PAGE_ARRAY;
        }
        long indexWithOffset = to + offset;
        int pageCount = pageIndex(indexWithOffset);
        int finalLimit = indexInPage(indexWithOffset);
        if (finalLimit != 0) {
            pageCount += 1;
        }

        Page[] duplicatePages = new Page[pageCount];
        Iterator<Page> pageIterator = this.pages.iterator();
        Page firstPage = pageIterator.next().duplicate();
        ByteBuffer firstBuffer = firstPage.byteBuffer();
        firstBuffer.position(firstBuffer.position() + offset);
        duplicatePages[0] = firstPage;
        for (int i = 1; i < duplicatePages.length; i++) {
            duplicatePages[i] = pageIterator.next().duplicate();
        }
        if (finalLimit != 0) {
            duplicatePages[duplicatePages.length - 1].byteBuffer().limit(finalLimit);
        }

        return duplicatePages;
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
            throw new IndexOutOfBoundsException(
                "can't slice a channel buffer with capacity [" + capacity + "], with slice parameters from [" + from + "]"
            );
        } else if (from == capacity) {
            return EMPTY_BYTE_BUFFER_ARRAY;
        }
        long indexWithOffset = from + offset;

        int pageIndex = pageIndex(indexWithOffset);
        int indexInPage = indexInPage(indexWithOffset);

        ByteBuffer[] buffers = new ByteBuffer[pages.size() - pageIndex];
        Iterator<Page> pageIterator = pages.descendingIterator();
        for (int i = buffers.length - 1; i > 0; --i) {
            buffers[i] = pageIterator.next().byteBuffer().duplicate();
        }
        ByteBuffer firstPostIndexBuffer = pageIterator.next().byteBuffer().duplicate();
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
            throw new IllegalArgumentException(
                "Cannot increment an index ["
                    + internalIndex
                    + "] with a delta ["
                    + delta
                    + "] that will result in a new index ["
                    + newIndex
                    + "] that is greater than the capacity ["
                    + capacity
                    + "]."
            );
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

    private int numPages(long requiredCapacity) {
        final long numPages = (requiredCapacity + PAGE_MASK) >>> PAGE_SHIFT;
        if (numPages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pageSize=" + (PAGE_MASK + 1) + " is too small for such as capacity: " + requiredCapacity);
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
