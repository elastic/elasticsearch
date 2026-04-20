/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * Builder for {@link PagedBytes}. Runs in one of three modes:
 * <ol>
 *     <li>
 *         When the bytes sequence is {@code <= BYTE_PAGE_SIZE / 2}
 *         it allocates an array on the heap. We call this {@link Mode#SMALL_TAIL} mode because
 *         {@code tail} is the name of the variable that holds the heap allocated array.
 *         And it is "small" while we're in this mode. And because it's cute.
 *     </li>
 *     <li>
 *         Otherwise run in {@link Mode#PAGED} mode and allocate pages of
 *         {@link PageCacheRecycler#BYTE_PAGE_SIZE}.
 *     </li>
 *     <li>
 *         After {@link #build()} completes, the builder enters {@link Mode#BUILT} mode.
 *         Ownership of all allocated memory is transferred to the returned {@link PagedBytes}.
 *         The only valid operation on the builder while in this mode is {@link #close},
 *         which is a noop.
 *     </li>
 * </ol>
 * <p>
 *     "Small tail" mode grows exponentially starting at 64 bytes. Then 128, then 256, on and on
 *     until 8kb. When the next growth would exceed {@link #MAX_SMALL_TAIL_SIZE} (8kb) we shift
 *     to paged mode instead.
 * </p>
 * <p>
 *     This <strong>feels</strong> quite like:
 * </p>
 * <ul>
 *     <li>
 *         {@code BreakingBytesRefBuilder}, but runs more slowly to make sure
 *         it never allocated any single array bigger than 16kb.
 *     </li>
 *     <li>
 *         {@link BytesReference}, but there isn't all of the invokeinterface
 *         to worry about.
 *     </li>
 *     <li>
 *         {@link ByteArray}, but runs more quickly because it's append only,
 *         not random access. And doesn't have all the {@code invokevirtual}
 *         to worry about slowing you down.
 *     </li>
 * </ul>
 */
public class PagedBytesBuilder implements Accountable, Releasable, Comparable<PagedBytesBuilder> {
    // TODO investigate all users of BreakingBytesRefBuilder for if they should use this
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PagedBytesBuilder.class);
    static final int MIN_SIZE = 64;
    static final int MAX_SMALL_TAIL_SIZE = BYTE_PAGE_SIZE / 2;

    private final PageCacheRecycler recycler;
    private final CircuitBreaker breaker;
    private final String label;

    /**
     * Recycler pages. If this is {@code null} then we're in "small tail"
     * mode. Otherwise, entries {@code 0..usedPages-1} are in use.
     * Set to {@code null} by {@link #build()} — the builder is invalid after that.
     */
    private Recycler.V<byte[]>[] pages;

    /**
     * Number of recycler pages in use.
     */
    private int usedPages;

    /**
     * High-water mark for recycler pages allocated. May exceed {@link #usedPages} after
     * {@link #clear()} so that previously-allocated pages can be reused without going
     * back to the recycler.
     */
    private int allocatedPages;

    /**
     * The page currently being filled. When {@code pages == null} then we're
     * in "small tail" mode and this is a heap allocated array. Otherwise, we're in
     * "paged" mode and this is {@code pages[usedPages - 1].v()}.
     */
    private byte[] tail;

    /**
     * Number of bytes written into {@link #tail}.
     */
    private int tailOffset;

    /**
     * Build the builder.
     * <p>
     *     The way we use {@code initialCapacity} is a bit complex. First, read the class
     *     javadoc for an explanation of {@link Mode}. If the {@code initialCapacity} puts
     *     the builder in {@link Mode#SMALL_TAIL} mode then we do what you'd expect: we
     *     allocate an oversize array for the data. If the {@code initialCapacity} puts
     *     the builder in {@link Mode#PAGED} then we'll allocate an array large enough
     *     to hold all the pages required for that many bytes. It does <strong>not</strong>
     *     pre-allocate all the pages because it doesn't really save any time.
     * </p>
     * @param recycler source of recycled pages
     * @param breaker breaker used for accounting size
     * @param label the label to add to any circuit breaker failures
     */
    public PagedBytesBuilder(PageCacheRecycler recycler, CircuitBreaker breaker, String label, int initialCapacity) {
        this.recycler = recycler;
        this.breaker = breaker;
        this.label = label;
        int expandedCapacity = initialCapacity <= MIN_SIZE ? MIN_SIZE : nextPowerOfTwo(initialCapacity);
        if (expandedCapacity < MAX_SMALL_TAIL_SIZE) {
            initSmallTailMode(expandedCapacity);
        } else {
            allocatePages(initialCapacity, SHALLOW_SIZE);
        }
    }

    /**
     * Append a byte.
     */
    public void append(byte b) {
        if (growTail(1)) {
            appendToTail(b);
        } else {
            appendPaged(b);
        }
    }

    /**
     * Append bytes.
     */
    public void append(byte[] b, int off, int len) {
        if (growTail(len)) {
            appendToTail(b, off, len);
        } else {
            appendPaged(b, off, len);
        }
    }

    private void appendToTail(byte b) {
        tail[tailOffset++] = b;
    }

    private void appendToTail(byte[] b, int off, int len) {
        System.arraycopy(b, off, tail, tailOffset, len);
        tailOffset += len;
    }

    private void appendToTail(int v) {
        INT.set(tail, tailOffset, v);
        tailOffset += Integer.BYTES;
    }

    private void appendToTail(long v) {
        LONG.set(tail, tailOffset, v);
        tailOffset += Long.BYTES;
    }

    private void appendNotToTail(byte[] b, int off, int len) {
        for (int i = 0; i < len; i++) {
            tail[tailOffset + i] = (byte) ~b[off + i];
        }
        tailOffset += len;
    }

    private void appendPaged(byte b) {
        if (tailOffset == tail.length) {
            nextPage();
        }
        appendToTail(b);
    }

    private void appendPaged(byte[] b, int off, int len) {
        while (len > 0) {
            if (tailOffset == tail.length) {
                nextPage();
            }
            int toCopy = Math.min(tail.length - tailOffset, len);
            System.arraycopy(b, off, tail, tailOffset, toCopy);
            tailOffset += toCopy;
            off += toCopy;
            len -= toCopy;
        }
    }

    private void appendPaged(int v) {
        appendPaged((byte) (v >> 24));
        appendPaged((byte) (v >> 16));
        appendPaged((byte) (v >> 8));
        appendPaged((byte) v);
    }

    private void appendPaged(long v) {
        appendPaged((byte) (v >> 56));
        appendPaged((byte) (v >> 48));
        appendPaged((byte) (v >> 40));
        appendPaged((byte) (v >> 32));
        appendPaged((byte) (v >> 24));
        appendPaged((byte) (v >> 16));
        appendPaged((byte) (v >> 8));
        appendPaged((byte) v);
    }

    private void appendNotPaged(byte[] b, int off, int len) {
        while (len > 0) {
            if (tailOffset == tail.length) {
                nextPage();
            }
            int toCopy = Math.min(tail.length - tailOffset, len);
            appendNotToTail(b, off, toCopy);
            off += toCopy;
            len -= toCopy;
        }
    }

    /**
     * Append the bitwise NOT of bytes.
     */
    public void appendNot(byte[] b, int off, int len) {
        if (growTail(len)) {
            appendNotToTail(b, off, len);
        } else {
            appendNotPaged(b, off, len);
        }
    }

    /**
     * Append bytes.
     */
    public void append(BytesRef b) {
        append(b.bytes, b.offset, b.length);
    }

    /**
     * Append bytes.
     */
    public void append(PagedBytesBuilder b) {
        for (int i = 0; i < b.usedPages - 1; i++) {
            append(b.pages[i].v(), 0, BYTE_PAGE_SIZE);
        }
        if (b.tail != null) {
            append(b.tail, 0, b.tailOffset);
        }
    }

    /**
     * Append bytes.
     */
    public void append(PagedBytes b) {
        int remaining = b.length();
        for (byte[] page : b.pages()) {
            int toCopy = Math.min(page.length, remaining);
            append(page, 0, toCopy);
            remaining -= toCopy;
        }
    }

    /**
     * Append an int in big-endian order.
     */
    public void append(int v) {
        if (growTail(Integer.BYTES)) {
            appendToTail(v);
        } else {
            appendPaged(v);
        }
    }

    /**
     * Append a long in big-endian order.
     */
    public void append(long v) {
        if (growTail(Long.BYTES)) {
            appendToTail(v);
        } else {
            appendPaged(v);
        }
    }

    /**
     * Append an int in variable-length format. Writes between one and five bytes.
     * Smaller values take fewer bytes. Negative numbers always use all 5 bytes.
     */
    public void appendVInt(int value) {
        if (growTail(Integer.BYTES + 1)) {
            appendVIntToTail(value);
        } else {
            appendVIntPaged(value);
        }
    }

    private void appendVIntToTail(int value) {
        while ((value & ~0x7F) != 0) {
            appendToTail((byte) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        appendToTail((byte) value);
    }

    private void appendVIntPaged(int value) {
        while ((value & ~0x7F) != 0) {
            appendPaged((byte) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        appendPaged((byte) value);
    }

    /**
     * Total bytes written so far.
     */
    public int length() {
        int length = tailOffset;
        if (usedPages > 1) {
            length += (usedPages - 1) * BYTE_PAGE_SIZE;
        }
        return length;
    }

    /**
     * Reset to zero length without changing mode. In small-tail mode the heap array
     * is kept at its current capacity. In paged mode all allocated pages are kept
     * for reuse on the next append.
     */
    public void clear() {
        assert mode() != Mode.BUILT : "clear() called on a built PagedBytesBuilder";
        if (pages != null && usedPages > 1) {
            usedPages = 1;
            tail = pages[0].v();
        }
        tailOffset = 0;
    }

    /**
     * Returns a {@link PagedBytesCursor} view into this builder's current contents.
     * The builder retains ownership of all memory.
     * Do not modify the builder while holding a reference to the returned view.
     */
    public PagedBytesCursor view(PagedBytesCursor scratch) {
        assert mode() != Mode.BUILT : "view() called on a built PagedBytesBuilder";
        int len = length();
        if (len == 0) {
            return PagedBytes.EMPTY.cursor(scratch);
        }
        if (usedPages == 0) {
            scratch.init(tail, 0, tailOffset);
            return scratch;
        }
        byte[][] bytePages = new byte[usedPages][];
        for (int i = 0; i < usedPages; i++) {
            bytePages[i] = pages[i].v();
        }
        scratch.init(bytePages, 0, 0, len, true);
        return scratch;
    }

    /**
     * Build a {@link PagedBytes} from the bytes written so far. Transfers ownership
     * of all allocated memory (either {@link #tail} in small-tail mode or {@link #pages}
     * in paged mode) to the result — this builder is invalid after this call.
     */
    public PagedBytes build() {
        if (length() == 0) {
            close();
            moveToBuilt();
            return PagedBytes.EMPTY;
        }
        if (usedPages == 0) {
            // Small case: only a small tail, no recycler pages.
            PagedBytes result = new PagedBytes(new byte[][] { tail }, tailOffset, new Releasable() {
                private final long charge = ramBytesUsed();

                @Override
                public void close() {
                    breaker.addWithoutBreaking(-charge);
                }
            });
            moveToBuilt();
            return result;
        }
        byte[][] bytePages = new byte[usedPages][];
        for (int i = 0; i < usedPages; i++) {
            bytePages[i] = pages[i].v();
        }
        PagedBytes result = new PagedBytes(bytePages, length(), new Releasable() {
            private final Recycler.V<byte[]>[] recycledPages = pages;
            private final long charge = ramBytesUsed();

            @Override
            public void close() {
                Releasables.close(Releasables.wrap(recycledPages), () -> breaker.addWithoutBreaking(-charge));
            }
        });
        moveToBuilt();
        return result;
    }

    private void moveToBuilt() {
        pages = null;
        usedPages = 0;
        allocatedPages = 0;
        tail = null;
        tailOffset = 0;
        assert mode() == Mode.BUILT;
    }

    /**
     * Try to grow the tail to have room for an {@code append} operation adding
     * {@code needed} bytes. Returns {@code true} if the bytes fit in the tail.
     * Returns {@code false} if the bytes must be split across two pages.
     * <p>
     *     If this returns {@code false} then we are in {@link Mode#PAGED} mode.
     *     If this returns {@code true} then we may be in {@link Mode#PAGED} mode
     *     or {@link Mode#SMALL_TAIL} mode. The caller shouldn't care - it can
     *     just write the bytes to the {@link #tail} regardless of how we've
     *     allocated it.
     * </p>
     * <p>
     *     If we need to grow the tail to more than {@link #MAX_SMALL_TAIL_SIZE}
     *     then this transitions to {@link Mode#PAGED} mode.
     * </p>
     * <p>
     *     To get the bytes to fit into the tail there are a few options:
     * </p>
     * {@snippet lang=text:
     *  ┌─────────────────────────────────────────┐
     *  │      needed bytes fit in the tail?      │
     *  └─────────────────────────────────────────┘
     *            yes │                 │ no
     *                ▼                 ▼
     *          return true    ┌─────────────────┐
     *                         │   paged mode?   │
     *                         └─────────────────┘
     *                      yes │              │ no
     *                          ▼              ▼
     *                     return false  ┌─────────────────────────────┐
     *                                   │ small tail mode:            │
     *                                   │ grow to nextPowerOfTwo(end) │
     *                                   │ would flip to paged mode?   │
     *                                   └─────────────────────────────┘
     *                                       yes │             │ no
     *                                           ▼             ▼
     *                                ╔════════════════╗  ┌────────────┐
     *                                ║   TRANSITION   ║  │  grow the  │
     *                                ║    TO PAGED    ║  │ small tail │
     *                                ║      MODE      ║  └────────────┘
     *                                ╚════════════════╝        │
     *                                           │              │
     *                                           ▼              ▼
     *                                     return false    return true
     * }
     */
    private boolean growTail(int needed) {
        int end = tailOffset + needed;
        if (end <= tail.length) {
            // Fits in the tail
            return true;
        }
        // Got to grow.
        if (pages != null) {
            // Already in paged mode. Growth always involves adding pages.
            return false;
        }
        int length = nextPowerOfTwo(end);
        if (length > MAX_SMALL_TAIL_SIZE) {
            // Would grow too large
            promoteToPaged(length);
            return end <= BYTE_PAGE_SIZE;
        }
        growSmallTail(length);
        return true;
    }

    private void nextPage() {
        maybeGrowPagesArray();
        if (usedPages < allocatedPages) {
            tail = pages[usedPages++].v();
        } else {
            grabNextPageFromRecycler();
        }
        tailOffset = 0;
    }

    private void maybeGrowPagesArray() {
        if (usedPages < pages.length) {
            return;
        }
        int newLength = ArrayUtil.oversize(pages.length + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        int oldLength = pages.length;
        breaker.addEstimateBytesAndMaybeBreak(pagesRamBytesUsed(newLength), label);
        pages = Arrays.copyOf(pages, newLength);
        breaker.addWithoutBreaking(-pagesRamBytesUsed(oldLength));
    }

    private void grabNextPageFromRecycler() {
        breaker.addEstimateBytesAndMaybeBreak(PAGE_RAM_BYTES_USED, label);
        Recycler.V<byte[]> v = recycler.bytePage(false);
        pages[usedPages++] = v;
        allocatedPages = usedPages;
        tail = v.v();
    }

    /**
     * Promote from {@link Mode#SMALL_TAIL} mode to {@link Mode#PAGED} mode.
     */
    private void promoteToPaged(int length) {
        assert mode() == Mode.SMALL_TAIL;

        byte[] oldTail = tail;
        allocatePages(length, 0);
        System.arraycopy(oldTail, 0, tail, 0, tailOffset);
        breaker.addWithoutBreaking(-smallTailRamBytesUsed(oldTail.length));
    }

    @SuppressWarnings("unchecked")
    private void allocatePages(int needed, long extraBytesToReserve) {
        int size = (needed + BYTE_PAGE_SIZE - 1) / BYTE_PAGE_SIZE;
        assert size > 0;
        breaker.addEstimateBytesAndMaybeBreak(extraBytesToReserve + pagesRamBytesUsed(size), label);
        boolean success = false;
        try {
            pages = new Recycler.V[size];
            grabNextPageFromRecycler();
            success = true;
        } finally {
            if (success == false) {
                pages = null;
                breaker.addWithoutBreaking(-extraBytesToReserve - pagesRamBytesUsed(size));
            }
        }
    }

    /**
     * Grow the {@link #tail} array via heap allocation.
     */
    private void growSmallTail(int length) {
        assert length <= MAX_SMALL_TAIL_SIZE;
        breaker.addEstimateBytesAndMaybeBreak(smallTailRamBytesUsed(length), label);
        int oldLength = tail.length;
        tail = Arrays.copyOf(tail, length);
        breaker.addWithoutBreaking(-smallTailRamBytesUsed(oldLength));
    }

    private void initSmallTailMode(int length) {
        assert length <= MAX_SMALL_TAIL_SIZE;
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE + smallTailRamBytesUsed(length), label);
        tail = new byte[length];
    }

    private static int nextPowerOfTwo(int n) {
        // Next power of two.
        return 1 << (32 - Integer.numberOfLeadingZeros(n - 1));
    }

    /**
     * Ram bytes used by each page.
     */
    static final long PAGE_RAM_BYTES_USED = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + BYTE_PAGE_SIZE);

    /**
     * Ram bytes used by {@link #tail} when it is heap allocated. When {@link #tail}
     * isn't heap allocated, we track it as another page in {@link #pages}.
     */
    private static long smallTailRamBytesUsed(int size) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + size);
    }

    /**
     * Ram bytes used by the {@link #pages}.
     */
    private static long pagesRamBytesUsed(int capacity) {
        return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) capacity * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != PagedBytesBuilder.class) {
            return false;
        }
        PagedBytesBuilder rhs = (PagedBytesBuilder) obj;
        if (this.length() != rhs.length()) {
            return false;
        }
        return compareTo(rhs) == 0;
    }

    @Override
    public int hashCode() {
        if (pages == null) {
            // Small-tail mode: all bytes are in a single contiguous array.
            return StringHelper.murmurhash3_x86_32(tail, 0, tailOffset, StringHelper.GOOD_FAST_HASH_SEED);
        }
        MurmurHash3x86_32 hasher = new MurmurHash3x86_32();
        for (int i = 0; i < usedPages - 1; i++) {
            hasher.fullPage(pages[i].v());
        }
        return hasher.lastPage(tail, tailOffset);
    }

    @Override
    public int compareTo(PagedBytesBuilder rhs) {
        int remaining = Math.min(this.length(), rhs.length());
        int fullPages = remaining / BYTE_PAGE_SIZE;
        int tailLen = remaining % BYTE_PAGE_SIZE;

        for (int page = 0; page < fullPages; page++) {
            int diff = Arrays.compareUnsigned(this.pages[page].v(), rhs.pages[page].v());
            if (diff != 0) {
                return diff;
            }
        }

        if (tailLen > 0) {
            byte[] lhsTail = this.usedPages > 0 ? this.pages[fullPages].v() : this.tail;
            byte[] rhsTail = rhs.usedPages > 0 ? rhs.pages[fullPages].v() : rhs.tail;
            int diff = Arrays.compareUnsigned(lhsTail, 0, tailLen, rhsTail, 0, tailLen);
            if (diff != 0) {
                return diff;
            }
        }

        // All shared bytes are the same.
        return Integer.compare(this.length(), rhs.length());
    }

    enum Mode {
        /** Heap-allocated tail, no recycler pages. */
        SMALL_TAIL,
        /** One or more recycler pages in use. */
        PAGED,
        /**
         * {@link #build()} has been called; ownership of all memory was transferred
         * to the returned {@link PagedBytes}.
         */
        BUILT,
    }

    Mode mode() {
        if (pages != null) {
            return Mode.PAGED;
        }
        if (tail != null) {
            return Mode.SMALL_TAIL;
        }
        return Mode.BUILT;
    }

    @Override
    public long ramBytesUsed() {
        return switch (mode()) {
            case BUILT -> 0;
            case SMALL_TAIL -> SHALLOW_SIZE + smallTailRamBytesUsed(tail.length);
            case PAGED -> SHALLOW_SIZE + pagesRamBytesUsed(pages.length) + (long) allocatedPages * PAGE_RAM_BYTES_USED;
        };
    }

    @Override
    public String toString() {
        return view(new PagedBytesCursor()).toString();
    }

    @Override
    public void close() {
        if (mode() == Mode.BUILT) {
            return;
        }
        long charge = ramBytesUsed();
        if (pages != null) {
            Releasables.close(pages);
            pages = null;
        }
        tail = null;
        if (charge > 0) {
            breaker.addWithoutBreaking(-charge);
        }
    }
}
