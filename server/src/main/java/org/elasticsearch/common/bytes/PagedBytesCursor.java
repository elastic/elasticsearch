/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * A sequential-read cursor over a sequence of {@code byte[]} pages. Tracks the current page
 * index and offset within that page. Callers advance the cursor by calling the
 * {@code read*} methods.
 * <p>
 *     This <strong>feels</strong> like {@link PagedBytesReference#streamInput()} but has
 *     <strong>much</strong> lower overhead. The combination of {@link #slice} and
 *     {@link #copyPageInto} make it quite useful for ESQL as well.
 * </p>
 */
public class PagedBytesCursor {
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    /**
     * Pages of bytes we're reading. It's quite possible to make a
     * {@link PagedBytesCursor} that slices from a very large list of
     * pages and only reads a few. In that case this is
     * <strong>still</strong> the entire list of pages, but
     * {@link #pageIndex} will be initialized to the page to read and
     * {@link #remaining} won't cover all bytes in the pages.
     */
    private byte[][] pages;
    /**
     * Index into {@link #pages} of the current page.
     */
    private int pageIndex;
    /**
     * Offset into the current page.
     */
    private int pageOffset;
    /**
     * Bytes remaining to be read across all pages.
     */
    private int remaining;
    /**
     * When this is {@code true} all pages except the last one are exactly
     * {@link PageCacheRecycler#BYTE_PAGE_SIZE} in length. And the last one
     * is {@code <= BYTE_PAGE_SIZE} in length. We use this far
     * faster {@link #slice}.
     */
    private boolean recyclerSizedPages;

    /**
     * Used by {@link #init(byte[], int, int)} to hold the single page so
     * we only have to allocate one time.
     */
    private byte[][] singlePageHolder;

    /**
     * Make an empty cursor, pointing at nothing. Use a variant of
     * {@link #init} to point it at something.
     */
    public PagedBytesCursor() {}

    /**
     * Reset this cursor to point at {@code remaining} bytes starting at
     * {@code pageIndex}/{@code pageOffset} within {@code pages}.
     *
     * @param recyclerSizedPages {@code true} if all pages except the last are exactly
     *                           {@link PageCacheRecycler#BYTE_PAGE_SIZE} bytes, enabling
     *                           arithmetic page advancement in {@link #slice}.
     *                           Pass {@code false} for pages of unknown or variable size.
     */
    public void init(byte[][] pages, int pageIndex, int pageOffset, int remaining, boolean recyclerSizedPages) {
        this.pages = pages;
        this.pageIndex = pageIndex;
        this.pageOffset = pageOffset;
        this.remaining = remaining;
        this.recyclerSizedPages = recyclerSizedPages;
    }

    /**
     * Reset this cursor to point at {@code remaining} bytes within a single {@code byte[]},
     * starting at {@code pageOffset}. Lazily allocates the single-page holder on first use.
     * {@link #recyclerSizedPages} is inferred from whether the page fits within
     * {@link PageCacheRecycler#BYTE_PAGE_SIZE}.
     */
    public void init(byte[] bytes, int pageOffset, int remaining) {
        if (singlePageHolder == null) {
            singlePageHolder = new byte[1][];
        }
        singlePageHolder[0] = bytes;
        this.pages = singlePageHolder;
        this.pageIndex = 0;
        this.pageOffset = pageOffset;
        this.remaining = remaining;
        this.recyclerSizedPages = bytes.length <= BYTE_PAGE_SIZE;
    }

    /**
     * Number of bytes not yet read.
     */
    public int remaining() {
        return remaining;
    }

    /**
     * Read one byte and advance.
     */
    public byte readByte() {
        if (remaining <= 0) {
            throw new IllegalArgumentException("no bytes remaining");
        }
        byte b = pages[pageIndex][pageOffset++];
        remaining--;
        if (pageOffset >= pages[pageIndex].length && remaining > 0) {
            pageIndex++;
            pageOffset = 0;
        }
        return b;
    }

    /**
     * Read an {@code int} and advance.
     */
    public int readInt() {
        if (remaining < Integer.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        if (pages[pageIndex].length - pageOffset >= Integer.BYTES) {
            int v = (int) INT.get(pages[pageIndex], pageOffset);
            pageOffset += Integer.BYTES;
            remaining -= Integer.BYTES;
            if (pageOffset >= pages[pageIndex].length && remaining > 0) {
                pageIndex++;
                pageOffset = 0;
            }
            return v;
        }
        return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16) | ((readByte() & 0xFF) << 8) | (readByte() & 0xFF);
    }

    /**
     * Read a {@code long} and advance.
     */
    public long readLong() {
        if (remaining < Long.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        if (pages[pageIndex].length - pageOffset >= Long.BYTES) {
            long v = (long) LONG.get(pages[pageIndex], pageOffset);
            pageOffset += Long.BYTES;
            remaining -= Long.BYTES;
            if (pageOffset >= pages[pageIndex].length && remaining > 0) {
                pageIndex++;
                pageOffset = 0;
            }
            return v;
        }
        return ((long) (readByte() & 0xFF) << 56) | ((long) (readByte() & 0xFF) << 48) | ((long) (readByte() & 0xFF) << 40)
            | ((long) (readByte() & 0xFF) << 32) | ((long) (readByte() & 0xFF) << 24) | ((long) (readByte() & 0xFF) << 16)
            | ((long) (readByte() & 0xFF) << 8) | (long) (readByte() & 0xFF);
    }

    /**
     * Read an int stored in variable-length format. Reads between one and five bytes.
     * Smaller values take fewer bytes. Negative numbers always use all 5 bytes.
     */
    public int readVInt() {
        byte b = readByte();
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = readByte();
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = readByte();
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = readByte();
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = readByte();
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) != 0) {
            throw new IllegalStateException("Invalid last byte for a vint [" + Integer.toHexString(b) + "]");
        }
        return i;
    }

    /**
     * Read bytes up to the end of the current page without crossing a page boundary,
     * returning a zero-copy {@link BytesRef} into the backing page. Advances the cursor.
     */
    public BytesRef readPageChunk(BytesRef scratch) {
        int len = Math.min(pages[pageIndex].length - pageOffset, remaining);
        scratch.bytes = pages[pageIndex];
        scratch.offset = pageOffset;
        scratch.length = len;
        pageOffset += len;
        remaining -= len;
        if (pageOffset >= pages[pageIndex].length && remaining > 0) {
            pageIndex++;
            pageOffset = 0;
        }
        return scratch;
    }

    /**
     * Write bytes up to the end of the current page into {@code dest} at {@code destOffset},
     * without crossing a page boundary. Advances the cursor.
     *
     * @return the number of bytes written
     */
    public int copyPageInto(ByteArray dest, long destOffset) {
        int len = Math.min(pages[pageIndex].length - pageOffset, remaining);
        dest.set(destOffset, pages[pageIndex], pageOffset, len);
        pageOffset += len;
        remaining -= len;
        if (pageOffset >= pages[pageIndex].length && remaining > 0) {
            pageIndex++;
            pageOffset = 0;
        }
        return len;
    }

    /**
     * Flip all bits of the remaining unread bytes in the underlying pages in-place.
     * The cursor position is not advanced.
     * <p>
     *     <strong>This mutates the underlying byte arrays.</strong> Callers must ensure
     *     they own or are otherwise permitted to modify the backing pages before calling
     *     this method.
     * </p>
     */
    public void bitwiseNot() {
        int rem = remaining;
        int pi = pageIndex;
        int po = pageOffset;
        while (rem > 0) {
            int len = Math.min(pages[pi].length - po, rem);
            for (int i = po; i < po + len; i++) {
                pages[pi][i] = (byte) ~pages[pi][i];
            }
            rem -= len;
            pi++;
            po = 0;
        }
    }

    /**
     * Point {@code scratch} at the next {@code len} bytes and advance this cursor past them.
     * The returned cursor shares the same backing pages — no copy is made.
     */
    public PagedBytesCursor slice(int len, PagedBytesCursor scratch) {
        if (remaining < len) {
            throw new IllegalArgumentException("not enough bytes");
        }
        scratch.pages = pages;
        scratch.pageIndex = pageIndex;
        scratch.pageOffset = pageOffset;
        scratch.remaining = len;
        scratch.recyclerSizedPages = recyclerSizedPages;
        if (recyclerSizedPages) {
            sliceRecyclerPages(len);
        } else {
            sliceUnknownPages(len);
        }
        remaining -= len;
        return scratch;
    }

    /**
     * Point {@code scratch} at the bytes up to (not including) {@code terminator} and advance
     * this cursor past the terminator. The returned cursor shares the same backing pages —
     * no copy is made.
     */
    public PagedBytesCursor sliceTerminated(byte terminator, PagedBytesCursor scratch) {
        int len = findTerminator(terminator);
        PagedBytesCursor result = slice(len, scratch);
        readByte(); // consume the terminator
        return result;
    }

    /**
     * Advance this cursor by {@code len} bytes using arithmetic page advancement.
     */
    private void sliceRecyclerPages(int len) {
        int abs = pageOffset + len;
        pageIndex += abs / BYTE_PAGE_SIZE;
        pageOffset = abs % BYTE_PAGE_SIZE;
    }

    /**
     * Advance this cursor by {@code len} bytes by walking through pages one at a time.
     * Safe for pages of any size.
     */
    private void sliceUnknownPages(int len) {
        int toSkip = len;
        while (toSkip > 0) {
            int avail = pages[pageIndex].length - pageOffset;
            if (toSkip < avail) {
                pageOffset += toSkip;
                return;
            }
            toSkip -= avail;
            pageIndex++;
            pageOffset = 0;
        }
    }

    private int findTerminator(byte terminator) {
        int scanPageIndex = pageIndex;
        int scanOffset = pageOffset;
        int scanRemaining = remaining;
        int len = 0;
        while (scanRemaining > 0) {
            if (pages[scanPageIndex][scanOffset] == terminator) {
                return len;
            }
            len++;
            scanOffset++;
            scanRemaining--;
            if (scanOffset >= pages[scanPageIndex].length && scanRemaining > 0) {
                scanPageIndex++;
                scanOffset = 0;
            }
        }
        throw new IllegalArgumentException("terminator not found");
    }

    /**
     * Compares the remaining bytes of this cursor with those of another.
     * Not intended for use in performance-sensitive code.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof PagedBytesCursor == false) {
            return false;
        }
        PagedBytesCursor other = (PagedBytesCursor) obj;
        if (remaining != other.remaining) {
            return false;
        }
        int pi = pageIndex;
        int po = pageOffset;
        int opi = other.pageIndex;
        int opo = other.pageOffset;
        int rem = remaining;
        while (rem > 0) {
            if (pages[pi][po] != other.pages[opi][opo]) {
                return false;
            }
            rem--;
            po++;
            opo++;
            if (po >= pages[pi].length && rem > 0) {
                pi++;
                po = 0;
            }
            if (opo >= other.pages[opi].length && rem > 0) {
                opi++;
                opo = 0;
            }
        }
        return true;
    }

    /**
     * Hashes the remaining unread bytes using an algorithm matching {@link BytesRef#hashCode()}.
     * Not intended for use in performance-sensitive code.
     */
    @Override
    public int hashCode() {
        MurmurHash3x86_32 hasher = new MurmurHash3x86_32();
        if (remaining > 0 && pageOffset == 0 && recyclerSizedPages) {
            int pi = pageIndex;
            int rem = remaining;
            while (rem > BYTE_PAGE_SIZE) {
                hasher.fullPage(pages[pi]);
                rem -= BYTE_PAGE_SIZE;
                pi++;
            }
            return hasher.lastPage(pages[pi], rem);
        }
        // Cursor starts mid-page or pages are variable-sized: copy to a flat array for the tail
        byte[] flat = new byte[remaining];
        int pi = pageIndex;
        int po = pageOffset;
        int rem = remaining;
        int dst = 0;
        while (rem > 0) {
            int len = Math.min(pages[pi].length - po, rem);
            System.arraycopy(pages[pi], po, flat, dst, len);
            dst += len;
            rem -= len;
            pi++;
            po = 0;
        }
        return hasher.lastPage(flat, flat.length);
    }

    /**
     * Returns a string representing the first 100 remaining bytes,
     * optionally followed by a count of remaining unrendered bytes. Examples:
     * <ul>
     *     <li>{@code [48 65 6C 6C 6F]}</li>
     *     <li>{@code [48 65 6C 6C 6F ...2.1kb more...]}</li>
     * </ul>
     */
    @Override
    public String toString() {
        int show = Math.min(remaining, 100);
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        int pi = pageIndex;
        int po = pageOffset;
        for (int i = 0; i < show; i++) {
            int b = pages[pi][po] & 0xFF;
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(Character.toUpperCase(Character.forDigit(b >> 4, 16)));
            sb.append(Character.toUpperCase(Character.forDigit(b & 0xF, 16)));
            po++;
            if (po >= pages[pi].length && i < show - 1) {
                pi++;
                po = 0;
            }
        }
        if (remaining > 100) {
            sb.append(" ...").append(ByteSizeValue.ofBytes(remaining - 100)).append(" more...");
        }
        return sb.append(']').toString();
    }
}
